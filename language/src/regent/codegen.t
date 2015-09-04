-- Copyright 2015 Stanford University, NVIDIA Corporation
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Legion Code Generation

local ast = require("regent/ast")
local log = require("regent/log")
local std = require("regent/std")
local symbol_table = require("regent/symbol_table")
local traverse_symbols = require("regent/traverse_symbols")
local cudahelper

-- Configuration Variables

-- Setting this flag to true allows the compiler to emit aligned
-- vector loads and stores, and is safe for use only with the general
-- LLR (because the shared LLR does not properly align instances).
local aligned_instances = std.config["aligned-instances"]

-- Setting this flag to true allows the compiler to use cached index
-- iterators, which are generally faster (they only walk the index
-- space bitmask once) but are only safe when the index space itself
-- is never modified (by allocator or deleting elements).
local cache_index_iterator = std.config["cached-iterators"]

-- Setting this flag to true directs the compiler to emit assertions
-- whenever two regions being placed in different physical regions
-- would require the use of the divergence-safe code path to be used.
local dynamic_branches_assert = std.config["no-dynamic-branches-assert"]

-- Setting this flag directs the compiler to emit bounds checks on all
-- pointer accesses. This is independent from the runtime's bounds
-- checks flag as the compiler does not use standard runtime
-- accessors.
local bounds_checks = std.config["bounds-checks"]

if std.config["cuda"] then cudahelper = require("regent/cudahelper") end

local codegen = {}

-- load Legion dynamic library
local c = std.c

local context = {}
context.__index = context

function context:new_local_scope(div)
  if div == nil then
    div = self.divergence
  end
  return setmetatable({
    expected_return_type = self.expected_return_type,
    constraints = self.constraints,
    task = self.task,
    task_meta = self.task_meta,
    leaf = self.leaf,
    divergence = div,
    context = self.context,
    runtime = self.runtime,
    ispaces = self.ispaces:new_local_scope(),
    regions = self.regions:new_local_scope(),
  }, context)
end

function context:new_task_scope(expected_return_type, constraints, leaf, task_meta, task, ctx, runtime)
  assert(expected_return_type and task and ctx and runtime)
  return setmetatable({
    expected_return_type = expected_return_type,
    constraints = constraints,
    task = task,
    task_meta = task_meta,
    leaf = leaf,
    divergence = nil,
    context = ctx,
    runtime = runtime,
    ispaces = symbol_table.new_global_scope({}),
    regions = symbol_table.new_global_scope({}),
  }, context)
end

function context.new_global_scope()
  return setmetatable({
  }, context)
end

function context:check_divergence(region_types, field_paths)
  if not self.divergence then
    return false
  end
  for _, divergence in ipairs(self.divergence) do
    local contained = true
    for _, r in ipairs(region_types) do
      if not divergence.group[r] then
        contained = false
        break
      end
    end
    for _, field_path in ipairs(field_paths) do
      if not divergence.valid_fields[std.hash(field_path)] then
        contained = false
        break
      end
    end
    if contained then
      return true
    end
  end
  return false
end

local ispace = setmetatable({}, { __index = function(t, k) error("ispace has no field " .. tostring(k), 2) end})
ispace.__index = ispace

function context:has_ispace(ispace_type)
  if not rawget(self, "ispaces") then
    error("not in task context", 2)
  end
  return self.ispaces:safe_lookup(ispace_type)
end

function context:ispace(ispace_type)
  if not rawget(self, "ispaces") then
    error("not in task context", 2)
  end
  return self.ispaces:lookup(nil, ispace_type)
end

function context:add_ispace_root(ispace_type, index_space, index_allocator,
                                 index_iterator)
  if not self.ispaces then
    error("not in task context", 2)
  end
  if self:has_ispace(ispace_type) then
    error("ispace " .. tostring(ispace_type) .. " already defined in this context", 2)
  end
  self.ispaces:insert(
    nil,
    ispace_type,
    setmetatable(
      {
        index_space = index_space,
        index_partition = nil,
        index_allocator = index_allocator,
        index_iterator = index_iterator,
        root_ispace_type = ispace_type,
      }, ispace))
end

function context:add_ispace_subispace(ispace_type, index_space, index_allocator,
                                      index_iterator, parent_ispace_type)
  if not self.ispaces then
    error("not in task context", 2)
  end
  if self:has_ispace(ispace_type) then
    error("ispace " .. tostring(ispace_type) .. " already defined in this context", 2)
  end
  if not self:ispace(parent_ispace_type) then
    error("parent to ispace " .. tostring(ispace_type) .. " not defined in this context", 2)
  end
  self.ispaces:insert(
    nil,
    ispace_type,
    setmetatable(
      {
        index_space = index_space,
        index_allocator = index_allocator,
        index_iterator = index_iterator,
        root_ispace_type = self:ispace(parent_ispace_type).root_ispace_type,
      }, ispace))
end

local region = setmetatable({}, { __index = function(t, k) error("region has no field " .. tostring(k), 2) end})
region.__index = region

function context:has_region(region_type)
  if not rawget(self, "regions") then
    error("not in task context", 2)
  end
  return self.regions:safe_lookup(region_type)
end

function context:region(region_type)
  if not rawget(self, "regions") then
    error("not in task context", 2)
  end
  return self.regions:lookup(nil, region_type)
end

function context:add_region_root(region_type, logical_region, field_paths,
                                 privilege_field_paths, field_privileges, field_types,
                                 field_ids, physical_regions,
                                 base_pointers, strides)
  if not self.regions then
    error("not in task context", 2)
  end
  if self:has_region(region_type) then
    error("region " .. tostring(region_type) .. " already defined in this context", 2)
  end
  if not self:has_ispace(region_type:ispace()) then
    error("ispace of region " .. tostring(region_type) .. " not defined in this context", 2)
  end
  self.regions:insert(
    nil,
    region_type,
    setmetatable(
      {
        logical_region = logical_region,
        default_partition = nil,
        default_product = nil,
        field_paths = field_paths,
        privilege_field_paths = privilege_field_paths,
        field_privileges = field_privileges,
        field_types = field_types,
        field_ids = field_ids,
        physical_regions = physical_regions,
        base_pointers = base_pointers,
        strides = strides,
        root_region_type = region_type,
      }, region))
end

function context:add_region_subregion(region_type, logical_region,
                                      default_partition, default_product,
                                      parent_region_type)
  if not self.regions then
    error("not in task context", 2)
  end
  if self:has_region(region_type) then
    error("region " .. tostring(region_type) .. " already defined in this context", 2)
  end
  if not self:has_ispace(region_type:ispace()) then
    error("ispace of region " .. tostring(region_type) .. " not defined in this context", 2)
  end
  if not self:region(parent_region_type) then
    error("parent to region " .. tostring(region_type) .. " not defined in this context", 2)
  end
  self.regions:insert(
    nil,
    region_type,
    setmetatable(
      {
        logical_region = logical_region,
        default_partition = default_partition,
        default_product = default_product,
        field_paths = self:region(parent_region_type).field_paths,
        privilege_field_paths = self:region(parent_region_type).privilege_field_paths,
        field_privileges = self:region(parent_region_type).field_privileges,
        field_types = self:region(parent_region_type).field_types,
        field_ids = self:region(parent_region_type).field_ids,
        physical_regions = self:region(parent_region_type).physical_regions,
        base_pointers = self:region(parent_region_type).base_pointers,
        strides = self:region(parent_region_type).strides,
        root_region_type = self:region(parent_region_type).root_region_type,
      }, region))
end

function region:field_type(field_path)
  local field_type = self.field_types[field_path:hash()]
  assert(field_type)
  return field_type
end

function region:field_id(field_path)
  local field_id = self.field_ids[field_path:hash()]
  assert(field_id)
  return field_id
end

function region:physical_region(field_path)
  local physical_region = self.physical_regions[field_path:hash()]
  assert(physical_region)
  return physical_region
end

function region:base_pointer(field_path)
  local base_pointer = self.base_pointers[field_path:hash()]
  assert(base_pointer)
  return base_pointer
end

function region:stride(field_path)
  local stride = self.strides[field_path:hash()]
  assert(stride)
  return stride
end

local function physical_region_get_base_pointer(cx, index_type, field_type, field_id, privilege, physical_region)
  local get_accessor = c.legion_physical_region_get_field_accessor_generic
  local accessor_args = terralib.newlist({physical_region})
  if std.is_reduction_op(privilege) then
    get_accessor = c.legion_physical_region_get_accessor_generic
  else
    accessor_args:insert(field_id)
  end

  local base_pointer = terralib.newsymbol(&field_type, "base_pointer")
  if index_type:is_opaque() then
    local expected_stride = terralib.sizeof(field_type)

    -- Note: This function MUST NOT be inlined. The presence of the
    -- address-of operator in the body of function causes
    -- optimizations to be disabled on the base pointer. When inlined,
    -- this then extends to the caller's scope, causing a significant
    -- performance regression.
    local terra get_base(accessor : c.legion_accessor_generic_t) : &field_type
      var base : &opaque = nil
      var stride : c.size_t = [expected_stride]
      var ok = c.legion_accessor_generic_get_soa_parameters(
        [accessor], &base, &stride)

      std.assert(ok, "failed to get base pointer")
      std.assert(base ~= nil, "base pointer is nil")
      std.assert(stride == [expected_stride],
                 "stride does not match expected value")
      return [&field_type](base)
    end

    local actions = quote
      var accessor = [get_accessor]([accessor_args])
      var [base_pointer] = [get_base](accessor)
    end
    return actions, base_pointer, terralib.newlist({expected_stride})
  else
    local dim = index_type.dim
    local expected_stride = terralib.sizeof(field_type)

    local dims = std.range(2, dim + 1)
    local strides = terralib.newlist()
    strides:insert(expected_stride)
    for i = 2, dim do
      strides:insert(terralib.newsymbol(c.size_t, "stride" .. tostring(i)))
    end

    local rect_t = c["legion_rect_" .. tostring(dim) .. "d_t"]
    local domain_get_rect = c["legion_domain_get_rect_" .. tostring(dim) .. "d"]
    local raw_rect_ptr = c["legion_accessor_generic_raw_rect_ptr_" .. tostring(dim) .. "d"]

    local actions = quote
      var accessor = [get_accessor]([accessor_args])

      var region = c.legion_physical_region_get_logical_region([physical_region])
      var domain = c.legion_index_space_get_domain([cx.runtime], [cx.context], region.index_space)
      var rect = [domain_get_rect](domain)

      var subrect : rect_t
      var offsets : c.legion_byte_offset_t[dim]
      var [base_pointer] = [&field_type]([raw_rect_ptr](
          accessor, rect, &subrect, &(offsets[0])))

      -- Sanity check the outputs.
      std.assert(base_pointer ~= nil, "base pointer is nil")
      [std.range(dim):map(
         function(i)
           return quote
             std.assert(subrect.lo.x[i] == rect.lo.x[i], "subrect not equal to rect")
             std.assert(subrect.hi.x[i] == rect.hi.x[i], "subrect not equal to rect")
           end
         end)]
      std.assert(offsets[0].offset == [expected_stride],
                 "stride does not match expected value")

      -- Fix up the base pointer so it points to the origin (zero),
      -- regardless of where rect is located. This allows us to do
      -- pointer arithmetic later oblivious to what sort of a subrect
      -- we are working with.
      [std.range(dim):map(
         function(i)
           return quote
             [base_pointer] = [&field_type](([&int8]([base_pointer])) - rect.lo.x[i] * offsets[i].offset)
           end
         end)]

      [dims:map(
         function(i)
           return quote var [ strides[i] ] = offsets[i-1].offset end
         end)]
    end
    return actions, base_pointer, strides
  end
end

-- A expr is an object which encapsulates a value and some actions
-- (statements) necessary to produce said value.
local expr = {}
expr.__index = function(t, k) error("expr: no such field " .. tostring(k), 2) end

function expr.just(actions, value)
  if not actions or not value then
    error("expr requires actions and value", 2)
  end
  return setmetatable({ actions = actions, value = value }, expr)
end

function expr.once_only(actions, value)
  if not actions or not value then
    error("expr requires actions and value", 2)
  end
  local value_name = terralib.newsymbol()
  actions = quote
    [actions]
    var [value_name] = [value]
  end
  return expr.just(actions, value_name)
end

-- A value encapsulates an rvalue or lvalue. Values are unwrapped by
-- calls to read or write, as appropriate for the lvalue-ness of the
-- object.

local values = {}

local function unpack_region(cx, region_expr, region_type, static_region_type)
  assert(not cx:has_region(region_type))

  local r = terralib.newsymbol(region_type, "r")
  local lr = terralib.newsymbol(c.legion_logical_region_t, "lr") 
  local is = terralib.newsymbol(c.legion_index_space_t, "is")
  local isa = false
  if not cx.leaf then
    isa = terralib.newsymbol(c.legion_index_allocator_t, "isa")
  end
  local it = false
  if cache_index_iterator then
    it = terralib.newsymbol(c.legion_terra_cached_index_iterator_t, "it")
  end
  local actions = quote
    [region_expr.actions]
    var [r] = [std.implicit_cast(
                 static_region_type, region_type, region_expr.value)]
    var [lr] = [r].impl
  end

  if not cx.leaf then
    actions = quote
      [actions]
      var [is] = [lr].index_space
      var [isa] = c.legion_index_allocator_create(
        [cx.runtime], [cx.context], [is])
    end
  end

  if cache_index_iterator then
    actions = quote
      [actions]
      var [it] = c.legion_terra_cached_index_iterator_create(
        [cx.runtime], [cx.context], [is])
    end
  end

  local parent_region_type = std.search_constraint_predicate(
    cx, region_type, {},
    function(cx, region)
      return cx:has_region(region)
    end)
  if not parent_region_type then
    error("failed to find appropriate for region " .. tostring(region_type) .. " in unpack", 2)
  end

  cx:add_ispace_subispace(region_type:ispace(), is, isa, it, parent_region_type:ispace())
  cx:add_region_subregion(region_type, r, false, false, parent_region_type)

  return expr.just(actions, r)
end

local value = {}
value.__index = value

function values.value(value_expr, value_type, field_path)
  if getmetatable(value_expr) ~= expr then
    error("value requires an expression", 2)
  end
  if not terralib.types.istype(value_type) then
    error("value requires a type", 2)
  end

  if field_path == nil then
    field_path = std.newtuple()
  elseif not std.is_tuple(field_path) then
    error("value requires a valid field_path", 2)
  end

  return setmetatable(
    {
      expr = value_expr,
      value_type = value_type,
      field_path = field_path,
    },
    value)
end

function value:new(value_expr, value_type, field_path)
  return values.value(value_expr, value_type, field_path)
end

function value:read(cx)
  local actions = self.expr.actions
  local result = self.expr.value
  for _, field_name in ipairs(self.field_path) do
    result = `([result].[field_name])
  end
  return expr.just(actions, result)
end

function value:write(cx, value)
  error("attempting to write to rvalue", 2)
end

function value:reduce(cx, value, op)
  error("attempting to reduce to rvalue", 2)
end

function value:__get_field(cx, value_type, field_name)
  if value_type:ispointer() then
    return values.rawptr(self:read(cx), value_type, std.newtuple(field_name))
  elseif std.is_index_type(value_type) then
    return self:new(self.expr, self.value_type, self.field_path .. std.newtuple("__ptr", field_name))
  elseif std.is_bounded_type(value_type) then
    if std.get_field(value_type.index_type.base_type, field_name) then
      return self:new(self.expr, self.value_type, self.field_path .. std.newtuple("__ptr", field_name))
    else
      assert(value_type:is_ptr())
      return values.ref(self:read(cx, value_type), value_type, std.newtuple(field_name))
    end
  elseif std.is_vptr(value_type) then
    return values.vref(self:read(cx, value_type), value_type, std.newtuple(field_name))
  else
    return self:new(
      self.expr, self.value_type, self.field_path .. std.newtuple(field_name))
  end
end

function value:get_field(cx, field_name, field_type)
  local value_type = self.value_type

  local result = self:unpack(cx, value_type, field_name, field_type)
  return result:__get_field(cx, value_type, field_name)
end

function value:get_index(cx, index, result_type)
  local value_expr = self:read(cx)
  local actions = terralib.newlist({value_expr.actions, index.actions})
  local value_type = std.as_read(self.value_type)
  if bounds_checks and value_type:isarray() then
    actions:insert(
      quote
        std.assert([index.value] >= 0 and [index.value] < [value_type.N],
          ["array access to " .. tostring(value_type) .. " is out-of-bounds"])
      end)
  end
  local result = expr.just(quote [actions] end,
                           `([value_expr.value][ [index.value] ]))
  return values.rawref(result, &result_type, std.newtuple())
end

function value:unpack(cx, value_type, field_name, field_type)
  local unpack_type = std.as_read(field_type)
  if std.is_region(unpack_type) and not cx:has_region(unpack_type) then
    local static_region_type = std.get_field(value_type, field_name)
    local region_expr = self:__get_field(cx, value_type, field_name):read(cx)
    region_expr = unpack_region(cx, region_expr, unpack_type, static_region_type)
    region_expr = expr.just(region_expr.actions, self.expr.value)
    return self:new(region_expr, self.value_type, self.field_path)
  elseif std.is_bounded_type(unpack_type) then
    assert(unpack_type:is_ptr())
    local region_types = unpack_type:bounds()

    do
      local has_all_regions = true
      for _, region_type in ipairs(region_types) do
        if not cx:has_region(region_type) then
          has_all_regions = false
          break
        end
      end
      if has_all_regions then
        return self
      end
    end

    -- FIXME: What to do about multi-region pointers?
    assert(#region_types == 1)
    local region_type = region_types[1]

    local static_ptr_type = std.get_field(value_type, field_name)
    local static_region_types = static_ptr_type:bounds()
    assert(#static_region_types == 1)
    local static_region_type = static_region_types[1]

    local region_field_name
    for _, entry in pairs(value_type:getentries()) do
      local entry_type = entry[2] or entry.type
      if entry_type == static_region_type then
        region_field_name = entry[1] or entry.field
      end
    end
    assert(region_field_name)

    local region_expr = self:__get_field(cx, value_type, region_field_name):read(cx)
    region_expr = unpack_region(cx, region_expr, region_type, static_region_type)
    region_expr = expr.just(region_expr.actions, self.expr.value)
    return self:new(region_expr, self.value_type, self.field_path)
  else
    return self
  end
end

local ref = setmetatable({}, { __index = value })
ref.__index = ref

function values.ref(value_expr, value_type, field_path)
  if not terralib.types.istype(value_type) or
    not (std.is_bounded_type(value_type) or std.is_vptr(value_type)) then
    error("ref requires a legion ptr type", 2)
  end
  return setmetatable(values.value(value_expr, value_type, field_path), ref)
end

function ref:new(value_expr, value_type, field_path)
  return values.ref(value_expr, value_type, field_path)
end

local function get_element_pointer(cx, region_types, index_type, field_type,
                                   base_pointer, strides, index)
  if bounds_checks then
    local terra check(runtime : c.legion_runtime_t,
                      ctx : c.legion_context_t,
                      pointer : c.legion_ptr_t,
                      pointer_index : uint32,
                      region : c.legion_logical_region_t,
                      region_index : uint32)
      if region_index == pointer_index then
        var check = c.legion_ptr_safe_cast(runtime, ctx, pointer, region)
        if c.legion_ptr_is_null(check) then
          std.assert(false, ["pointer " .. tostring(index_type) .. " is out-of-bounds"])
        end
      end
      return pointer
    end

    local pointer_value
    if not index_type.fields then
      -- Currently unchecked.
    elseif #index_type.fields == 1 then
      local field = index_type.fields[1]
      pointer_value = `(c.legion_ptr_t { value = [index].__ptr.[field] })
    else
      -- Currently unchecked.
    end

    local pointer_index = 1
    if #region_types > 1 then
      pointer_index = `([index].__index)
    end

    if pointer_value then
      for region_index, region_type in ipairs(region_types) do
        assert(cx:has_region(region_type))
        local lr = cx:region(region_type).logical_region
        index = `([index_type] {
            __ptr = check(
              [cx.runtime], [cx.context],
              [pointer_value], [pointer_index],
              [lr].impl, [region_index])})
      end
    end
  end

  -- Note: This code is performance-critical and tends to be sensitive
  -- to small changes. Please thoroughly performance-test any changes!
  if not index_type.fields then
    -- Assumes stride[1] == terralib.sizeof(field_type)
    return `(@[&field_type](&base_pointer[ [index].__ptr ]))
  elseif #index_type.fields == 1 then
    -- Assumes stride[1] == terralib.sizeof(field_type)
    local field = index_type.fields[1]
    return `(@[&field_type](&base_pointer[ [index].__ptr.[field] ]))
  else
    local offset
    for i, field in ipairs(index_type.fields) do
      if offset then
        offset = `(offset + [index].__ptr.[ field ] * [ strides[i] ])
      else
        offset = `([index].__ptr.[ field ] * [ strides[i] ])
      end
    end
    return `(@([&field_type]([&int8](base_pointer) + offset)))
  end
end

function ref:__ref(cx, expr_type)
  local actions = self.expr.actions
  local value = self.expr.value

  local value_type = std.as_read(
    std.get_field_path(self.value_type.points_to_type, self.field_path))
  local field_paths, field_types = std.flatten_struct_fields(value_type)
  local absolute_field_paths = field_paths:map(
    function(field_path) return self.field_path .. field_path end)

  local region_types = self.value_type:bounds()
  local base_pointers_by_region = region_types:map(
    function(region_type)
      return absolute_field_paths:map(
        function(field_path)
          return cx:region(region_type):base_pointer(field_path)
        end)
    end)
  local strides_by_region = region_types:map(
    function(region_type)
      return absolute_field_paths:map(
        function(field_path)
          return cx:region(region_type):stride(field_path)
        end)
    end)

  local base_pointers, strides

  if cx.check_divergence(region_types, field_paths) or #region_types == 1 then
    base_pointers = base_pointers_by_region[1]
    strides = strides_by_region[1]
  else
    base_pointers = std.zip(absolute_field_paths, field_types):map(
      function(field)
        local field_path, field_type = unpack(field)
        return terralib.newsymbol(&field_type, "base_pointer_" .. field_path:hash())
      end)
    strides = absolute_field_paths:map(
      function(field_path)
        return cx:region(region_types[1]):stride(field_path):map(
          function(_)
            return terralib.newsymbol(c.size_t, "stride_" .. field_path:hash())
          end)
      end)

    local cases
    for i = #region_types, 1, -1 do
      local region_base_pointers = base_pointers_by_region[i]
      local region_strides = strides_by_region[i]
      local case = std.zip(base_pointers, region_base_pointers, strides, region_strides):map(
        function(pair)
          local base_pointer, region_base_pointer, field_strides, field_region_strides = unpack(pair)
          local setup = quote [base_pointer] = [region_base_pointer] end
          for i, stride in ipairs(field_strides) do
            local region_stride = field_region_strides[i]
            setup = quote [setup]; [stride] = [region_stride] end
          end
          return setup
        end)

      if cases then
        cases = quote
          if [value].__index == [i] then
            [case]
          else
            [cases]
          end
        end
      else
        cases = case
      end
    end

    actions = quote
      [actions];
      [base_pointers:map(
         function(base_pointer) return quote var [base_pointer] end end)];
      [strides:map(
         function(stride) return quote [stride:map(function(s) return quote var [s] end end)] end end)];
      [cases]
    end
  end

  local values
  if not expr_type or std.as_read(expr_type) == value_type then
    values = std.zip(field_types, base_pointers, strides):map(
      function(field)
        local field_type, base_pointer, stride = unpack(field)
        return get_element_pointer(cx, region_types, self.value_type, field_type, base_pointer, stride, value)
      end)
  else
    assert(expr_type:isvector() or std.is_vptr(expr_type) or std.is_sov(expr_type))
    values = std.zip(field_types, base_pointers, strides):map(
      function(field)
        local field_type, base_pointer, stride = unpack(field)
        local vec = vector(field_type, std.as_read(expr_type).N)
        return `(@[&vec](&[get_element_pointer(cx, region_types, self.value_type, field_type, base_pointer, stride, value)]))
      end)
    value_type = expr_type
  end

  return actions, values, value_type, field_paths, field_types
end

function ref:read(cx, expr_type)
  if expr_type and (std.is_ref(expr_type) or std.is_rawref(expr_type)) then
    expr_type = std.as_read(expr_type)
  end
  local actions, values, value_type, field_paths, field_types = self:__ref(cx, expr_type)
  local value = terralib.newsymbol(value_type)
  actions = quote
    [actions];
    var [value] : value_type
    [std.zip(values, field_paths, field_types):map(
       function(pair)
         local field_value, field_path, field_type = unpack(pair)
         local result = value
         for _, field_name in ipairs(field_path) do
           result = `([result].[field_name])
         end
         if expr_type and
            (expr_type:isvector() or
             std.is_vptr(expr_type) or
             std.is_sov(expr_type)) then
           if field_type:isvector() then field_type = field_type.type end
           local align = sizeof(field_type)
           if aligned_instances then
             align = sizeof(vector(field_type, expr_type.N))
           end
           return quote
             [result] = terralib.attrload(&[field_value], {align = [align]})
           end
         else
           return quote [result] = [field_value] end
         end
      end)]
  end
  return expr.just(actions, value)
end

function ref:write(cx, value, expr_type)
  if expr_type and (std.is_ref(expr_type) or std.is_rawref(expr_type)) then
    expr_type = std.as_read(expr_type)
  end
  local value_expr = value:read(cx, expr_type)
  local actions, values, value_type, field_paths, field_types = self:__ref(cx, expr_type)
  actions = quote
    [value_expr.actions];
    [actions];
    [std.zip(values, field_paths, field_types):map(
       function(pair)
         local field_value, field_path, field_type = unpack(pair)
         local result = value_expr.value
         for _, field_name in ipairs(field_path) do
           result = `([result].[field_name])
         end
         if expr_type and
            (expr_type:isvector() or
             std.is_vptr(expr_type) or
             std.is_sov(expr_type)) then
           if field_type:isvector() then field_type = field_type.type end
           local align = sizeof(field_type)
           if aligned_instances then
             align = sizeof(vector(field_type, expr_type.N))
           end
           return quote
             terralib.attrstore(&[field_value], [result], {align = [align]})
           end
         else
          return quote [field_value] = [result] end
        end
      end)]
  end
  return expr.just(actions, quote end)
end

local reduction_fold = {
  ["+"] = "+",
  ["-"] = "-",
  ["*"] = "*",
  ["/"] = "*", -- FIXME: Need to fold with "/" for RW instances.
  ["max"] = "max",
  ["min"] = "min",
}

function ref:reduce(cx, value, op, expr_type)
  if expr_type and (std.is_ref(expr_type) or std.is_rawref(expr_type)) then
    expr_type = std.as_read(expr_type)
  end
  local fold_op = reduction_fold[op]
  assert(fold_op)
  local value_expr = value:read(cx, expr_type)
  local actions, values, value_type, field_paths, field_types = self:__ref(cx, expr_type)
  actions = quote
    [value_expr.actions];
    [actions];
    [std.zip(values, field_paths, field_types):map(
       function(pair)
         local field_value, field_path, field_type = unpack(pair)
         local result = value_expr.value
         for _, field_name in ipairs(field_path) do
           result = `([result].[field_name])
         end
         if expr_type and
            (expr_type:isvector() or
             std.is_vptr(expr_type) or
             std.is_sov(expr_type)) then
           if field_type:isvector() then field_type = field_type.type end
           local align = sizeof(field_type)
           if aligned_instances then
             align = sizeof(vector(field_type, expr_type.N))
           end

           local field_value_load = quote
              terralib.attrload(&[field_value], {align = [align]})
           end
           local sym = terralib.newsymbol()
           return quote
             var [sym] : expr_type =
               terralib.attrload(&[field_value], {align = [align]})
             terralib.attrstore(&[field_value],
               [std.quote_binary_op(fold_op, sym, result)],
               {align = [align]})
           end
         else
           return quote
             [field_value] = [std.quote_binary_op(
                                fold_op, field_value, result)]
           end
         end
      end)]
  end
  return expr.just(actions, quote end)
end

function ref:get_field(cx, field_name, field_type, value_type)
  assert(value_type)
  value_type = std.as_read(value_type)

  local result = self:unpack(cx, value_type, field_name, field_type)
  return result:__get_field(cx, value_type, field_name)
end

function ref:get_index(cx, index, result_type)
  local value_actions, value = self:__ref(cx)
  -- Arrays are never field-sliced, therefore, an array array access
  -- must be to a single field.
  assert(#value == 1)
  value = value[1]

  local actions = terralib.newlist({value_actions, index.actions})
  local value_type = self.value_type.points_to_type
  if bounds_checks and value_type:isarray() then
    actions:insert(
      quote
        std.assert([index.value] >= 0 and [index.value] < [value_type.N],
          ["array access to " .. tostring(value_type) .. " is out-of-bounds"])
      end)
  end
  local result = expr.just(quote [actions] end, `([value][ [index.value] ]))
  return values.rawref(result, &result_type, std.newtuple())
end

local vref = setmetatable({}, { __index = value })
vref.__index = vref

function values.vref(value_expr, value_type, field_path)
  if not terralib.types.istype(value_type) or not std.is_vptr(value_type) then
    error("vref requires a legion vptr type", 2)
  end
  return setmetatable(values.value(value_expr, value_type, field_path), vref)
end

function vref:new(value_expr, value_type, field_path)
  return values.vref(value_expr, value_type, field_path)
end

function vref:__unpack(cx)
  assert(std.is_vptr(self.value_type))

  local actions = self.expr.actions
  local value = self.expr.value

  local value_type = std.as_read(
    std.get_field_path(self.value_type.points_to_type, self.field_path))
  local field_paths, field_types = std.flatten_struct_fields(value_type)
  local absolute_field_paths = field_paths:map(
    function(field_path) return self.field_path .. field_path end)

  local region_types = self.value_type:bounds()
  local base_pointers_by_region = region_types:map(
    function(region_type)
      return absolute_field_paths:map(
        function(field_path)
          return cx:region(region_type):base_pointer(field_path)
        end)
    end)

  return field_paths, field_types, region_types, base_pointers_by_region
end

function vref:read(cx, expr_type)
  if expr_type and (std.is_ref(expr_type) or std.is_rawref(expr_type)) then
    expr_type = std.as_read(expr_type)
  end
  assert(expr_type:isvector() or std.is_vptr(expr_type) or std.is_sov(expr_type))
  local actions = self.expr.actions
  local field_paths, field_types, region_types, base_pointers_by_region = self:__unpack(cx)
  -- where the result should go
  local value = terralib.newsymbol(expr_type)
  local vref_value = self.expr.value
  local vector_width = self.value_type.N

  -- make symols to store scalar values from different pointers
  local vars = terralib.newlist()
  for i = 1, vector_width do
    local v = terralib.newsymbol(expr_type.type)
    vars:insert(v)
    actions = quote
      [actions];
      var [ v ] : expr_type.type
    end
  end

  -- if the vptr points to a single region
  if cx.check_divergence(region_types, field_paths) or #region_types == 1 then
    local base_pointers = base_pointers_by_region[1]

    std.zip(base_pointers, field_paths):map(
      function(pair)
        local base_pointer, field_path = unpack(pair)
        for i = 1, vector_width do
          local v = vars[i]
          for _, field_name in ipairs(field_path) do
            v = `([v].[field_name])
          end
          actions = quote
            [actions];
            [v] = base_pointer[ [vref_value].__ptr.value[ [i - 1] ] ]
          end
        end
      end)
  -- if the vptr can point to multiple regions
  else
    for field_idx, field_path in pairs(field_paths) do
      for vector_idx = 1, vector_width do
        local v = vars[vector_idx]
        for _, field_name in ipairs(field_path) do
          v = `([v].[field_name])
        end
        local cases
        for region_idx = #base_pointers_by_region, 1, -1 do
          local base_pointer = base_pointers_by_region[region_idx][field_idx]
          local case = quote
              v = base_pointer[ [vref_value].__ptr.value[ [vector_idx - 1] ] ]
          end

          if cases then
            cases = quote
              if [vref_value].__index[ [vector_idx - 1] ] == [region_idx] then
                [case]
              else
                [cases]
              end
            end
          else
            cases = case
          end
        end
        actions = quote [actions]; [cases] end
      end
    end
  end

  actions = quote
    [actions];
    var [value] : expr_type
    [field_paths:map(
       function(field_path)
         local result = value
         local field_accesses = vars:map(
          function(v)
            for _, field_name in ipairs(field_path) do
              v = `([v].[field_name])
            end
            return v
          end)
         for _, field_name in ipairs(field_path) do
           result = `([result].[field_name])
         end
         return quote [result] = vector( [field_accesses] ) end
       end)]
  end

  return expr.just(actions, value)
end

function vref:write(cx, value, expr_type)
  if expr_type and (std.is_ref(expr_type) or std.is_rawref(expr_type)) then
    expr_type = std.as_read(expr_type)
  end
  assert(expr_type:isvector() or std.is_vptr(expr_type) or std.is_sov(expr_type))
  local actions = self.expr.actions
  local value_expr = value:read(cx, expr_type)
  local field_paths, field_types, region_types, base_pointers_by_region = self:__unpack(cx)

  local vref_value = self.expr.value
  local vector_width = self.value_type.N

  actions = quote
    [value_expr.actions];
    [actions]
  end

  if cx.check_divergence(region_types, field_paths) or #region_types == 1 then
    local base_pointers = base_pointers_by_region[1]

    std.zip(base_pointers, field_paths):map(
      function(pair)
        local base_pointer, field_path = unpack(pair)
        local result = value_expr.value
        for i = 1, vector_width do
          local field_value = `base_pointer[ [vref_value].__ptr.value[ [i - 1] ] ]
          for _, field_name in ipairs(field_path) do
            result = `([result].[field_name])
          end
          local assignment
          if value.value_type:isprimitive() then
            assignment = quote
              [field_value] = [result]
            end
          else
            assignment = quote
              [field_value] = [result][ [i - 1] ]
            end
          end
          actions = quote
            [actions];
            [assignment]
          end
        end
      end)
  else
    for field_idx, field_path in pairs(field_paths) do
      for vector_idx = 1, vector_width do
        local result = value_expr.value
        for _, field_name in ipairs(field_path) do
          result = `([result].[field_name])
        end
        if value.value_type:isvector() then
          result = `(result[ [vector_idx - 1] ])
        end
        local cases
        for region_idx = #base_pointers_by_region, 1, -1 do
          local base_pointer = base_pointers_by_region[region_idx][field_idx]
          local case = quote
            base_pointer[ [vref_value].__ptr.value[ [vector_idx - 1] ] ] =
              result
          end

          if cases then
            cases = quote
              if [vref_value].__index[ [vector_idx - 1] ] == [region_idx] then
                [case]
              else
                [cases]
              end
            end
          else
            cases = case
          end
        end
        actions = quote [actions]; [cases] end
      end
    end

  end
  return expr.just(actions, quote end)
end

function vref:reduce(cx, value, op, expr_type)
  if expr_type and (std.is_ref(expr_type) or std.is_rawref(expr_type)) then
    expr_type = std.as_read(expr_type)
  end
  assert(expr_type:isvector() or std.is_vptr(expr_type) or std.is_sov(expr_type))
  local actions = self.expr.actions
  local fold_op = reduction_fold[op]
  assert(fold_op)
  local value_expr = value:read(cx, expr_type)
  local field_paths, field_types, region_types, base_pointers_by_region = self:__unpack(cx)

  local vref_value = self.expr.value
  local vector_width = self.value_type.N

  actions = quote
    [value_expr.actions];
    [actions]
  end

  if cx.check_divergence(region_types, field_paths) or #region_types == 1 then
    local base_pointers = base_pointers_by_region[1]

    std.zip(base_pointers, field_paths):map(
      function(pair)
        local base_pointer, field_path = unpack(pair)
        local result = value_expr.value
        for i = 1, vector_width do
          local field_value = `base_pointer[ [vref_value].__ptr.value[ [i - 1] ] ]
          for _, field_name in ipairs(field_path) do
            result = `([result].[field_name])
          end
          if value.value_type:isprimitive() then
            actions = quote
              [actions];
              [field_value] =
                [std.quote_binary_op(fold_op, field_value, result)]
            end
          else
            local v = terralib.newsymbol()
            local assignment = quote
              var [v] = [result][ [i - 1] ]
            end
            actions = quote
              [actions];
              [assignment];
              [field_value] =
                [std.quote_binary_op(fold_op, field_value, v)]
            end
          end
        end
      end)
  else
    for field_idx, field_path in pairs(field_paths) do
      for vector_idx = 1, vector_width do
        local result = value_expr.value
        for _, field_name in ipairs(field_path) do
          result = `([result].[field_name])
        end
        if value.value_type:isvector() then
          result = `result[ [vector_idx - 1] ]
        end
        local cases
        for region_idx = #base_pointers_by_region, 1, -1 do
          local base_pointer = base_pointers_by_region[region_idx][field_idx]
          local field_value = `base_pointer[ [vref_value].__ptr.value[ [vector_idx - 1] ] ]
          local case = quote
            [field_value] =
              [std.quote_binary_op(fold_op, field_value, result)]
          end
          if cases then
            cases = quote
              if [vref_value].__index[ [vector_idx - 1] ] == [region_idx] then
                [case]
              else
                [cases]
              end
            end
          else
            cases = case
          end
        end
        actions = quote [actions]; [cases] end
      end
    end

  end

  return expr.just(actions, quote end)
end

function vref:get_field(cx, field_name, field_type, value_type)
  assert(value_type)
  value_type = std.as_read(value_type)

  local result = self:unpack(cx, value_type, field_name, field_type)
  return result:__get_field(cx, value_type, field_name)
end

local rawref = setmetatable({}, { __index = value })
rawref.__index = rawref

-- For pointer-typed rvalues, this entry point coverts the pointer
-- to an lvalue by dereferencing the pointer.
function values.rawptr(value_expr, value_type, field_path)
  if getmetatable(value_expr) ~= expr then
    error("rawref requires an expression", 2)
  end

  value_expr = expr.just(value_expr.actions, `(@[value_expr.value]))
  return values.rawref(value_expr, value_type, field_path)
end

-- This entry point is for lvalues which are already references
-- (e.g. for mutable variables on the stack). Conceptually
-- equivalent to a pointer rvalue which has been dereferenced. Note
-- that value_type is still the pointer type, not the reference
-- type.
function values.rawref(value_expr, value_type, field_path)
  if not terralib.types.istype(value_type) or not value_type:ispointer() then
    error("rawref requires a pointer type, got " .. tostring(value_type), 2)
  end
  return setmetatable(values.value(value_expr, value_type, field_path), rawref)
end

function rawref:new(value_expr, value_type, field_path)
  return values.rawref(value_expr, value_type, field_path)
end

function rawref:__ref(cx)
  local actions = self.expr.actions
  local result = self.expr.value
  for _, field_name in ipairs(self.field_path) do
    result = `([result].[field_name])
  end
  return expr.just(actions, result)
end

function rawref:read(cx)
  return self:__ref(cx)
end

function rawref:write(cx, value)
  local value_expr = value:read(cx)
  local ref_expr = self:__ref(cx)
  local actions = quote
    [value_expr.actions];
    [ref_expr.actions];
    [ref_expr.value] = [value_expr.value]
  end
  return expr.just(actions, quote end)
end

function rawref:reduce(cx, value, op)
  local ref_expr = self:__ref(cx)
  local value_expr = value:read(cx)

  local ref_type = self.value_type.type
  local value_type = std.as_read(value.value_type)

  local reduce = ast.typed.ExprBinary {
    op = op,
    lhs = ast.typed.ExprInternal {
      value = values.value(expr.just(quote end, ref_expr.value), ref_type),
      expr_type = ref_type,
    },
    rhs = ast.typed.ExprInternal {
      value = values.value(expr.just(quote end, value_expr.value), value_type),
      expr_type = value_type,
    },
    expr_type = ref_type,
    span = ast.trivial_span(),
  }

  local reduce_expr = codegen.expr(cx, reduce):read(cx, ref_type)

  local actions = quote
    [value_expr.actions];
    [ref_expr.actions];
    [reduce_expr.actions];
    [ref_expr.value] = [reduce_expr.value]
  end
  return expr.just(actions, quote end)
end

function rawref:get_field(cx, field_name, field_type, value_type)
  assert(value_type)
  value_type = std.as_read(value_type)

  local result = self:unpack(cx, value_type, field_name, field_type)
  return result:__get_field(cx, value_type, field_name)
end

function rawref:get_index(cx, index, result_type)
  local ref_expr = self:__ref(cx)
  local actions = terralib.newlist({ref_expr.actions, index.actions})
  local value_type = self.value_type.type
  if bounds_checks and value_type:isarray() then
    actions:insert(
      quote
        std.assert([index.value] >= 0 and [index.value] < [value_type.N],
          ["array access to " .. tostring(value_type) .. " is out-of-bounds"])
      end)
  end
  local result = expr.just(
    quote [actions] end,
    `([ref_expr.value][ [index.value] ]))
  return values.rawref(result, &result_type, std.newtuple())
end

-- A helper for capturing debug information.
function emit_debuginfo(node)
  assert(node.span.source and node.span.start.line)
  if string.len(node.span.source) == 0 then
    return quote end
  end
  return quote
    terralib.debuginfo(node.span.source, node.span.start.line)
  end
end

function codegen.expr_internal(cx, node)
  return node.value
end

function codegen.expr_id(cx, node)
  if std.is_rawref(node.expr_type) then
    return values.rawref(
      expr.just(emit_debuginfo(node), node.value),
      node.expr_type.pointer_type)
  else
    return values.value(
      expr.just(emit_debuginfo(node), node.value),
      node.expr_type)
  end
end

function codegen.expr_constant(cx, node)
  local value = node.value
  local value_type = std.as_read(node.expr_type)
  return values.value(
    expr.just(emit_debuginfo(node), `([terralib.constant(value_type, value)])),
    value_type)
end

function codegen.expr_function(cx, node)
  local value_type = std.as_read(node.expr_type)
  return values.value(
    expr.just(emit_debuginfo(node), node.value),
    value_type)
end

function codegen.expr_field_access(cx, node)
  local value_type = std.as_read(node.value.expr_type)
  if std.is_region(value_type) then
    local value = codegen.expr(cx, node.value):read(cx)
    local actions = quote
      [value.actions];
      [emit_debuginfo(node)]
    end

    assert(cx:has_region(value_type))
    local lp
    if value_type:has_default_partition() and node.field_name == "partition"
    then
      lp = cx:region(value_type).default_partition
    elseif value_type:has_default_product() and node.field_name == "product"
    then
      lp = cx:region(value_type).default_product
    end
    assert(lp)

    return values.value(
      expr.once_only(actions, lp),
      node.expr_type)
  else
    local field_name = node.field_name
    local field_type = node.expr_type
    return codegen.expr(cx, node.value):get_field(cx, field_name, field_type, node.value.expr_type)
  end
end

function codegen.expr_index_access(cx, node)
  local value_type = std.as_read(node.value.expr_type)
  local expr_type = std.as_read(node.expr_type)

  if std.is_partition(value_type) or std.is_cross_product(value_type) then
    local value = codegen.expr(cx, node.value):read(cx)
    local index = codegen.expr(cx, node.index):read(cx)

    local actions = quote
      [value.actions];
      [index.actions];
      [emit_debuginfo(node)]
    end

    if cx:has_region(expr_type) then
      local lr = cx:region(expr_type).logical_region
      if std.is_cross_product(value_type) then
        local ip = terralib.newsymbol(c.legion_index_partition_t, "ip")
        local lp = terralib.newsymbol(c.legion_logical_partition_t, "lp")
        actions = quote
          [actions]
          var [ip] = c.legion_terra_index_cross_product_get_subpartition_by_color(
            [cx.runtime], [cx.context],
            [value.value].product, [index.value])
          var [lp] = c.legion_logical_partition_create(
            [cx.runtime], [cx.context], [lr].impl, [ip])
        end

        if not cx:region(expr_type).default_partition then
          local subpartition_type = expr_type:default_partition()
          local subpartition = terralib.newsymbol(subpartition_type, "subpartition")

          actions = quote
            [actions]
            var [subpartition] = [subpartition_type] { impl = [lp] }
          end
          cx:region(expr_type).default_partition = subpartition
        end
        if #value_type:partitions() > 2 and not cx:region(expr_type).default_product then
          local subproduct_type = expr_type:default_product()
          local subproduct = terralib.newsymbol(subproduct_type, "subproduct")

          actions = quote
            [actions]
            var ip2 = [value.value].partitions[2]
            var [subproduct] = [subproduct_type] {
              impl = [lp],
              product = c.legion_terra_index_cross_product_t {
                partition = [ip],
                other = ip2,
              },
              -- FIXME: partitions
            }
          end
          cx:region(expr_type).default_product = subproduct
        end
      end
      return values.value(expr.just(actions, lr), expr_type)
    end

    local parent_region_type = value_type:parent_region()

    local r = terralib.newsymbol(expr_type, "r")
    local lr = terralib.newsymbol(c.legion_logical_region_t, "lr")
    local is = terralib.newsymbol(c.legion_index_space_t, "is")
    local isa = false
    if not cx.leaf then
      isa = terralib.newsymbol(c.legion_index_allocator_t, "isa")
    end
    local it = false
    if cache_index_iterator then
      it = terralib.newsymbol(c.legion_terra_cached_index_iterator_t, "it")
    end
    actions = quote
      [actions]
      var [lr] = c.legion_logical_partition_get_logical_subregion_by_color(
        [cx.runtime], [cx.context],
        [value.value].impl, [index.value])
      var [is] = [lr].index_space
      var [r] = [expr_type] { impl = [lr] }
    end

    if not cx.leaf then
      actions = quote
        [actions]
        var [isa] = c.legion_index_allocator_create(
          [cx.runtime], [cx.context], [is])
      end
    end

    if cache_index_iterator then
      actions = quote
        [actions]
        var [it] = c.legion_terra_cached_index_iterator_create(
          [cx.runtime], [cx.context], [is])
      end
    end

    local subpartition, subproduct = false, false
    if std.is_cross_product(value_type) then
      assert(expr_type:has_default_partition())
      local subpartition_type = expr_type:default_partition()
      subpartition = terralib.newsymbol(subpartition_type, "subpartition")

      local ip = terralib.newsymbol(c.legion_index_partition_t, "ip")
      local lp = terralib.newsymbol(c.legion_logical_partition_t, "lp")
      actions = quote
        [actions]
        var [ip] = c.legion_terra_index_cross_product_get_subpartition_by_color(
          [cx.runtime], [cx.context],
          [value.value].product, [index.value])
        var [lp] = c.legion_logical_partition_create(
          [cx.runtime], [cx.context], [lr], [ip])
        var [subpartition] = [subpartition_type] { impl = [lp] }
      end

      if expr_type:has_default_product() then
        local subproduct_type = expr_type:default_product()
        subproduct = terralib.newsymbol(subproduct_type, "subproduct")

        actions = quote
          [actions]
          var ip2 = [value.value].partitions[2]
          var [subproduct] = [subproduct_type] {
            impl = [lp],
            product = c.legion_terra_index_cross_product_t {
              partition = [ip],
              other = ip2,
            },
            -- FIXME: partitions
          }
        end
      end
    end

    cx:add_ispace_subispace(expr_type:ispace(), is, isa, it, parent_region_type:ispace())
    cx:add_region_subregion(expr_type, r, subpartition, subproduct, parent_region_type)

    return values.value(expr.just(actions, r), expr_type)
  elseif std.is_region(value_type) then
    local index = codegen.expr(cx, node.index):read(cx)
    return values.ref(index, node.expr_type.pointer_type)
  else
    local index = codegen.expr(cx, node.index):read(cx)
    return codegen.expr(cx, node.value):get_index(cx, index, expr_type)
  end
end

function codegen.expr_method_call(cx, node)
  local value = codegen.expr(cx, node.value):read(cx)
  local args = node.args:map(
    function(arg) return codegen.expr(cx, arg):read(cx) end)

  local actions = quote
    [value.actions];
    [args:map(function(arg) return arg.actions end)];
    [emit_debuginfo(node)]
  end
  local expr_type = std.as_read(node.expr_type)

  return values.value(
    expr.once_only(
      actions,
      `([value.value]:[node.method_name](
          [args:map(function(arg) return arg.value end)]))),
    expr_type)
end

function expr_call_setup_task_args(cx, task, args, arg_types, param_types,
                                   params_struct_type, params_map, task_args,
                                   task_args_setup)
  -- This all has to be done in 64-bit integers to avoid imprecision
  -- loss due to Lua's ONLY numeric type being double. Below we use
  -- LuaJIT's uint64_t cdata type as a replacement.

  -- Beware: LuaJIT does not expose bitwise operators at the Lua
  -- level. Below we use plus (instead of bitwise or) and
  -- exponentiation (instead of shift).
  local params_map_value = 0ULL
  for i, arg_type in ipairs(arg_types) do
    if std.is_future(arg_type) then
      params_map_value = params_map_value + (2ULL ^ (i-1))
    end
  end

  if params_map then
    task_args_setup:insert(quote
      [task_args].[params_map] = [params_map_value]
    end)
  end

  -- Prepare the by-value arguments to the task.
  for i, arg in ipairs(args) do
    local arg_type = arg_types[i]
    if not std.is_future(arg_type) then
      local c_field = params_struct_type:getentries()[i + 1]
      local c_field_name = c_field[1] or c_field.field
      if terralib.issymbol(c_field_name) then
        c_field_name = c_field_name.displayname
      end
      task_args_setup:insert(quote [task_args].[c_field_name] = [arg] end)
    end
  end

  -- Prepare the region arguments to the task.

  -- Pass field IDs by-value to the task.
  local param_field_ids = task:get_field_id_params()
  do
    local param_field_id_i = 1
    for _, i in ipairs(std.fn_param_regions_by_index(task:gettype())) do
      local arg_type = arg_types[i]
      local param_type = param_types[i]

      local field_paths, _ = std.flatten_struct_fields(param_type.fspace_type)
      for _, field_path in pairs(field_paths) do
        local arg_field_id = cx:region(arg_type):field_id(field_path)
        local param_field_id = param_field_ids[param_field_id_i]
        param_field_id_i = param_field_id_i + 1
        task_args_setup:insert(
          quote [task_args].[param_field_id] = [arg_field_id] end)
      end
    end
  end
  return task_args_setup
end

function expr_call_setup_future_arg(cx, task, arg, arg_type, param_type,
                                    launcher, index, future_args_setup)
  local add_future = c.legion_task_launcher_add_future
  if index then
    add_future = c.legion_index_launcher_add_future
  end

  future_args_setup:insert(quote
    add_future(launcher, [arg].__result)
  end)

  return future_args_setup
end

function expr_call_setup_ispace_arg(cx, task, arg_type, param_type, launcher,
                                    index, ispace_args_setup)
  local parent_ispace =
    cx:ispace(cx:ispace(arg_type).root_ispace_type).index_space

  local add_requirement
  if index then
      add_requirement = c.legion_index_launcher_add_index_requirement
  else
      add_requirement = c.legion_task_launcher_add_index_requirement
  end
  assert(add_requirement)

  local requirement = terralib.newsymbol("requirement")
  local requirement_args = terralib.newlist({
      launcher, `([cx:ispace(arg_type).index_space].impl),
      c.ALL_MEMORY, `([parent_ispace].impl), false})

  ispace_args_setup:insert(
    quote
      var [requirement] = [add_requirement]([requirement_args])
    end)
end

function expr_call_setup_region_arg(cx, task, arg_type, param_type, launcher,
                                    index, region_args_setup)
  local privileges, privilege_field_paths, privilege_field_types =
    std.find_task_privileges(param_type, task:getprivileges())
  local privilege_modes = privileges:map(std.privilege_mode)
  local parent_region =
    cx:region(cx:region(arg_type).root_region_type).logical_region

  local add_field = c.legion_task_launcher_add_field
  if index then
    add_field = c.legion_index_launcher_add_field
  end

  for i, privilege in ipairs(privileges) do
    local field_paths = privilege_field_paths[i]
    local field_types = privilege_field_types[i]
    local privilege_mode = privilege_modes[i]

    local reduction_op
    if std.is_reduction_op(privilege) then
      local op = std.get_reduction_op(privilege)
      assert(#field_types == 1)
      local field_type = field_types[1]
      reduction_op = std.reduction_op_ids[op][field_type]
    end

    if privilege_mode == c.REDUCE then
      assert(reduction_op)
    end

    local add_requirement
    if index then
      if reduction_op then
        add_requirement = c.legion_index_launcher_add_region_requirement_logical_region_reduction
     else
        add_requirement = c.legion_index_launcher_add_region_requirement_logical_region
      end
    else
      if reduction_op then
        add_requirement = c.legion_task_launcher_add_region_requirement_logical_region_reduction
      else
        add_requirement = c.legion_task_launcher_add_region_requirement_logical_region
      end
    end
    assert(add_requirement)

    local requirement = terralib.newsymbol("requirement")
    local requirement_args = terralib.newlist({
        launcher, `([cx:region(arg_type).logical_region].impl)})
    if index then
      requirement_args:insert(0)
    end
    if reduction_op then
      requirement_args:insert(reduction_op)
    else
      requirement_args:insert(privilege_mode)
    end
    requirement_args:insertall(
      {c.EXCLUSIVE, `([parent_region].impl), 0, false})

    region_args_setup:insert(
      quote
        var [requirement] = [add_requirement]([requirement_args])
        [field_paths:map(
           function(field_path)
             local field_id = cx:region(arg_type):field_id(field_path)
             return quote
               add_field(
                 [launcher], [requirement], [field_id], true)
             end
           end)]
      end)
  end
end

function expr_call_setup_partition_arg(cx, task, arg_type, param_type,
                                       partition, launcher, index,
                                       region_args_setup)
  assert(index)
  local privileges, privilege_field_paths, privilege_field_types =
    std.find_task_privileges(param_type, task:getprivileges())
  local privilege_modes = privileges:map(std.privilege_mode)
  local parent_region =
    cx:region(cx:region(arg_type).root_region_type).logical_region

  for i, privilege in ipairs(privileges) do
    local field_paths = privilege_field_paths[i]
    local field_types = privilege_field_types[i]
    local privilege_mode = privilege_modes[i]

    local reduction_op
    if std.is_reduction_op(privilege) then
      local op = std.get_reduction_op(privilege)
      assert(#field_types == 1)
      local field_type = field_types[1]
      reduction_op = std.reduction_op_ids[op][field_type]
    end

    if privilege_mode == c.REDUCE then
      assert(reduction_op)
    end

    local add_requirement
    if reduction_op then
      add_requirement = c.legion_index_launcher_add_region_requirement_logical_partition_reduction
    else
      add_requirement = c.legion_index_launcher_add_region_requirement_logical_partition
    end
    assert(add_requirement)

    local requirement = terralib.newsymbol("requirement")
    local requirement_args = terralib.newlist({
        launcher, `([partition].impl), 0 --[[ default projection ID ]]})
    if reduction_op then
      requirement_args:insert(reduction_op)
    else
      requirement_args:insert(privilege_mode)
    end
    requirement_args:insertall(
      {c.EXCLUSIVE, `([parent_region].impl), 0, false})

    region_args_setup:insert(
      quote
      var [requirement] =
        [add_requirement]([requirement_args])
        [field_paths:map(
           function(field_path)
             local field_id = cx:region(arg_type):field_id(field_path)
             return quote
               c.legion_index_launcher_add_field(
                 [launcher], [requirement], [field_id], true)
             end
           end)]
      end)
  end
end

function codegen.expr_call(cx, node)
  local fn = codegen.expr(cx, node.fn):read(cx)
  local args = node.args:map(
    function(arg) return codegen.expr(cx, arg):read(cx, arg.expr_type) end)

  local actions = quote
    [fn.actions];
    [args:map(function(arg) return arg.actions end)];
    [emit_debuginfo(node)]
  end

  local arg_types = terralib.newlist()
  for i, arg in ipairs(args) do
    arg_types:insert(std.as_read(node.args[i].expr_type))
  end

  local arg_values = terralib.newlist()
  local param_types = node.fn.expr_type.parameters
  for i, arg in ipairs(args) do
    local arg_value = args[i].value
    if i <= #param_types and param_types[i] ~= std.untyped and
      not std.is_future(arg_types[i])
    then
      arg_values:insert(std.implicit_cast(arg_types[i], param_types[i], arg_value))
    else
      arg_values:insert(arg_value)
    end
  end

  local value_type = std.as_read(node.expr_type)
  if std.is_task(fn.value) then
    local params_struct_type = fn.value:get_params_struct()
    local task_args = terralib.newsymbol(params_struct_type)
    local task_args_setup = terralib.newlist()
    expr_call_setup_task_args(
      cx, fn.value, arg_values, arg_types, param_types,
      params_struct_type, fn.value:get_params_map(),
      task_args, task_args_setup)

    local launcher = terralib.newsymbol("launcher")

    -- Pass futures.
    local future_args_setup = terralib.newlist()
    for i, arg_type in ipairs(arg_types) do
      if std.is_future(arg_type) then
        local arg_value = arg_values[i]
        local param_type = param_types[i]
        expr_call_setup_future_arg(
          cx, fn.value, arg_value, arg_type, param_type,
          launcher, false, future_args_setup)
      end
    end

    -- Pass index spaces through index requirements.
    local ispace_args_setup = terralib.newlist()
    for i, arg_type in ipairs(arg_types) do
      if std.is_ispace(arg_type) then
        local param_type = param_types[i]

        expr_call_setup_ispace_arg(
          cx, fn.value, arg_type, param_type, launcher, false, ispace_args_setup)
      end
    end

    -- Pass regions through region requirements.
    local region_args_setup = terralib.newlist()
    for _, i in ipairs(std.fn_param_regions_by_index(fn.value:gettype())) do
      local arg_type = arg_types[i]
      local param_type = param_types[i]

      expr_call_setup_region_arg(
        cx, fn.value, arg_type, param_type, launcher, false, region_args_setup)
    end

    local future = terralib.newsymbol("future")
    local launcher_setup = quote
      var [task_args]
      [task_args_setup]
      var t_args : c.legion_task_argument_t
      t_args.args = [&opaque](&[task_args])
      t_args.arglen = terralib.sizeof(params_struct_type)
      var [launcher] = c.legion_task_launcher_create(
        [fn.value:gettaskid()], t_args,
        c.legion_predicate_true(), 0, 0)
      [future_args_setup]
      [ispace_args_setup]
      [region_args_setup]
      var [future] = c.legion_task_launcher_execute(
        [cx.runtime], [cx.context], [launcher])
    end
    local launcher_cleanup = quote
      c.legion_task_launcher_destroy(launcher)
    end

    local future_type = value_type
    if not std.is_future(future_type) then
      future_type = std.future(value_type)
    end

    actions = quote
      [actions]
      [launcher_setup]
      [launcher_cleanup]
    end
    local future_value = values.value(
      expr.once_only(actions, `([future_type]{ __result = [future] })),
      value_type)

    if std.is_future(value_type) then
      return future_value
    elseif value_type == terralib.types.unit then
      actions = quote
        [actions]
        c.legion_future_destroy(future)
      end

      return values.value(expr.just(actions, quote end), terralib.types.unit)
    else
      return codegen.expr(
        cx,
        ast.typed.ExprFutureGetResult {
          value = ast.typed.ExprInternal {
            value = future_value,
            expr_type = future_type,
          },
          expr_type = value_type,
          span = node.span,
        })
    end
  else
    return values.value(
      expr.once_only(actions, `([fn.value]([arg_values]))),
      value_type)
  end
end

function codegen.expr_cast(cx, node)
  local fn = codegen.expr(cx, node.fn):read(cx)
  local arg = codegen.expr(cx, node.arg):read(cx, node.arg.expr_type)

  local actions = quote
    [fn.actions];
    [arg.actions];
    [emit_debuginfo(node)]
  end
  local value_type = std.as_read(node.expr_type)
  return values.value(
    expr.once_only(actions, `([fn.value]([arg.value]))),
    value_type)
end

function codegen.expr_ctor_list_field(cx, node)
  return codegen.expr(cx, node.value):read(cx)
end

function codegen.expr_ctor_rec_field(cx, node)
  return  codegen.expr(cx, node.value):read(cx)
end

function codegen.expr_ctor_field(cx, node)
  if node:is(ast.typed.ExprCtorListField) then
    return codegen.expr_ctor_list_field(cx, node)
  elseif node:is(ast.typed.ExprCtorRecField) then
    return codegen.expr_ctor_rec_field(cx, node)
  else
  end
end

function codegen.expr_ctor(cx, node)
  local fields = node.fields:map(
    function(field) return codegen.expr_ctor_field(cx, field) end)

  local field_values = fields:map(function(field) return field.value end)
  local actions = quote
    [fields:map(function(field) return field.actions end)];
    [emit_debuginfo(node)]
  end
  local expr_type = std.as_read(node.expr_type)

  if node.named then
    local st = std.ctor(
      node.fields:map(
        function(field)
          local field_type = std.as_read(field.value.expr_type)
          return { field.name, field_type }
        end))

    return values.value(
      expr.once_only(actions, `([st]({ [field_values] }))),
      expr_type)
  else
    return values.value(
      expr.once_only(actions, `({ [field_values] })),
      expr_type)
  end
end

function codegen.expr_raw_context(cx, node)
  local value_type = std.as_read(node.expr_type)
  return values.value(
    expr.just(emit_debuginfo(node), cx.context),
    value_type)
end

function codegen.expr_raw_fields(cx, node)
  local region = codegen.expr(cx, node.region):read(cx)
  local region_type = std.as_read(node.region.expr_type)
  local expr_type = std.as_read(node.expr_type)

  local region = cx:region(region_type)
  local field_ids = terralib.newlist()
  for i, field_path in ipairs(node.fields) do
    field_ids:insert({i-1, region:field_id(field_path)})
  end

  local result = terralib.newsymbol("raw_fields")
  local actions = quote
    [emit_debuginfo(node)]
    var [result] : expr_type
    [field_ids:map(
       function(pair)
         local i, field_id = unpack(pair)
         return quote [result][ [i] ] = [field_id] end
       end)]
  end

  return values.value(
    expr.just(actions, result),
    expr_type)
end

function codegen.expr_raw_physical(cx, node)
  local region = codegen.expr(cx, node.region):read(cx)
  local region_type = std.as_read(node.region.expr_type)
  local expr_type = std.as_read(node.expr_type)

  local region = cx:region(region_type)
  local physical_regions = terralib.newlist()
  for i, field_path in ipairs(node.fields) do
    physical_regions:insert({i-1, region:physical_region(field_path)})
  end

  local result = terralib.newsymbol("raw_physical")
  local actions = quote
    [emit_debuginfo(node)]
    var [result] : expr_type
    [physical_regions:map(
       function(pair)
         local i, physical_region = unpack(pair)
         return quote [result][ [i] ] = [physical_region] end
       end)]
  end

  return values.value(
    expr.just(actions, result),
    expr_type)
end

function codegen.expr_raw_runtime(cx, node)
  local value_type = std.as_read(node.expr_type)
  return values.value(
    expr.just(emit_debuginfo(node), cx.runtime),
    value_type)
end

function codegen.expr_raw_value(cx, node)
  local value = codegen.expr(cx, node.value):read(cx)
  local value_type = std.as_read(node.value.expr_type)
  local expr_type = std.as_read(node.expr_type)

  local actions = value.actions
  local result
  if std.is_ispace(value_type) then
    result = `([value.value].impl)
  elseif std.is_region(value_type) then
    result = `([value.value].impl)
  elseif std.is_partition(value_type) then
    result = `([value.value].impl)
  elseif std.is_cross_product(value_type) then
    result = `([value.value].product)
  else
    assert(false)
  end

  return values.value(
    expr.just(actions, result),
    expr_type)
end

function codegen.expr_isnull(cx, node)
  local pointer = codegen.expr(cx, node.pointer):read(cx)
  local expr_type = std.as_read(node.expr_type)
  local actions = quote
    [pointer.actions];
    [emit_debuginfo(node)]
  end

  return values.value(
    expr.once_only(
      actions,
      `([expr_type](c.legion_ptr_is_null([pointer.value].__ptr)))),
    expr_type)
end

function codegen.expr_new(cx, node)
  local pointer_type = node.pointer_type
  local region = codegen.expr(cx, node.region):read(cx)
  local region_type = std.as_read(node.region.expr_type)
  local ispace_type = region_type
  if std.is_region(region_type) then
    ispace_type = region_type:ispace()
  end
  assert(std.is_ispace(ispace_type))
  local isa = cx:ispace(ispace_type).index_allocator

  local expr_type = std.as_read(node.expr_type)
  local actions = quote
    [region.actions];
    [emit_debuginfo(node)]
  end

  return values.value(
    expr.once_only(
      actions,
      `([pointer_type]{ __ptr = c.legion_index_allocator_alloc([isa], 1) })),
    expr_type)
end

function codegen.expr_null(cx, node)
  local pointer_type = node.pointer_type
  local expr_type = std.as_read(node.expr_type)

  return values.value(
    expr.once_only(
      emit_debuginfo(node),
      `([pointer_type]{ __ptr = c.legion_ptr_nil() })),
    expr_type)
end

function codegen.expr_dynamic_cast(cx, node)
  local value = codegen.expr(cx, node.value):read(cx)
  local expr_type = std.as_read(node.expr_type)

  local actions = quote
    [value.actions];
    [emit_debuginfo(node)]
  end
  local input = `([value.value].__ptr)
  local result
  local regions = expr_type:bounds()
  if #regions == 1 then
    local region = regions[1]
    assert(cx:has_region(region))
    local lr = `([cx:region(region).logical_region].impl)
    result = `(
      [expr_type]({
          __ptr = (c.legion_ptr_safe_cast([cx.runtime], [cx.context], [input], [lr]))
      }))
  else
    result = terralib.newsymbol(expr_type)
    local cases = quote
      [result] = [expr_type]({ __ptr = c.legion_ptr_nil(), __index = 0 })
    end
    for i = #regions, 1, -1 do
      local region = regions[i]
      assert(cx:has_region(region))
      local lr = `([cx:region(region).logical_region].impl)
      cases = quote
        var temp = c.legion_ptr_safe_cast([cx.runtime], [cx.context], [input], [lr])
        if not c.legion_ptr_is_null(temp) then
          result = [expr_type]({
            __ptr = temp,
            __index = [i],
          })
        else
          [cases]
        end
      end
    end

    actions = quote [actions]; var [result]; [cases] end
  end

  return values.value(expr.once_only(actions, result), expr_type)
end

function codegen.expr_static_cast(cx, node)
  local value = codegen.expr(cx, node.value):read(cx)
  local value_type = std.as_read(node.value.expr_type)
  local expr_type = std.as_read(node.expr_type)

  local actions = quote
    [value.actions];
    [emit_debuginfo(node)]
  end
  local input = value.value
  local result
  if #(expr_type:bounds()) == 1 then
    result = terralib.newsymbol(expr_type)
    local input_regions = value_type:bounds()
    local result_last = node.parent_region_map[#input_regions]
    local cases
    if result_last then
      cases = quote
        [result] = [expr_type]({ __ptr = [input].__ptr })
      end
    else
      cases = quote
        [result] = [expr_type]({ __ptr = c.legion_ptr_nil() })
      end
    end
    for i = #input_regions - 1, 1, -1 do
      local result_i = node.parent_region_map[i]
      if result_i then
        cases = quote
          if [input].__index == [i] then
            [result] = [expr_type]({ __ptr = [input].__ptr })
          else
            [cases]
          end
        end
      else
        cases = quote
          if [input].__index == [i] then
            [result] = [expr_type]({ __ptr = c.legion_ptr_nil() })
          else
            [cases]
          end
        end
      end
    end

    actions = quote [actions]; var [result]; [cases] end
  else
    result = terralib.newsymbol(expr_type)
    local input_regions = value_type:bounds()
    local result_last = node.parent_region_map[#input_regions]
    local cases
    if result_last then
      cases = quote
        [result] = [expr_type]({
            __ptr = [input].__ptr,
            __index = [result_last],
        })
      end
    else
      cases = quote
        [result] = [expr_type]({
            __ptr = c.legion_ptr_nil(),
            __index = 0,
        })
      end
    end
    for i = #input_regions - 1, 1, -1 do
      local result_i = node.parent_region_map[i]
      if result_i then
        cases = quote
          if [input].__index == [i] then
            [result] = [expr_type]({
              __ptr = [input].__ptr,
              __index = [result_i],
            })
          else
            [cases]
          end
        end
      else
        cases = quote
          if [input].__index == [i] then
            [result] = [expr_type]({
              __ptr = c.legion_ptr_nil(),
              __index = 0,
            })
          else
            [cases]
          end
        end
      end
    end

    actions = quote [actions]; var [result]; [cases] end
  end

  return values.value(expr.once_only(actions, result), expr_type)
end

function codegen.expr_ispace(cx, node)
  local index_type = node.index_type
  local extent = codegen.expr(cx, node.extent):read(cx)
  local extent_type = std.as_read(node.extent.expr_type)
  local start = node.start and codegen.expr(cx, node.start):read(cx)
  local ispace_type = std.as_read(node.expr_type)
  local actions = quote
    [extent.actions];
    [start and start.actions or (quote end)];
    [emit_debuginfo(node)]
  end

  local extent_value = `([std.implicit_cast(extent_type, index_type, extent.value)].__ptr)
  if index_type:is_opaque() then
    extent_value = `([extent_value].value)
  end

  local start_value = start and `([std.implicit_cast(start_type, index_type, start.value)].__ptr)
  if index_type:is_opaque() then
    start_value = start and `([start_value].value)
  end

  local is = terralib.newsymbol(c.legion_index_space_t, "is")
  local i = terralib.newsymbol(ispace_type, "i")

  -- FIXME: Runtime does not understand how to make multi-dimensional
  -- index spaces allocable.
  local isa = false
  if ispace_type.dim == 0 then
    isa = terralib.newsymbol(c.legion_index_allocator_t, "isa")
  end
  local it = false
  if cache_index_iterator then
    it = terralib.newsymbol(c.legion_terra_cached_index_iterator_t, "it")
  end

  cx:add_ispace_root(ispace_type, i, isa, it)

  if ispace_type.dim == 0 then
    if start then
      actions = quote
        [actions]
        std.assert([start_value] == 0, "opaque ispaces must start at 0 right now")
      end
    end
    actions = quote
      [actions]
      var [is] = c.legion_index_space_create([cx.runtime], [cx.context], [extent_value])
      var [isa] = c.legion_index_allocator_create([cx.runtime], [cx.context],  [is])
    end

    if cache_index_iterator then
      actions = quote
        [actions]
        var [it] = c.legion_terra_cached_index_iterator_create(
          [cx.runtime], [cx.context], [is])
      end
    end
  else
    if not start then
      start_value = index_type:zero()
    end

    local domain_from_bounds = std["domain_from_bounds_" .. tostring(ispace_type.dim) .. "d"]
    actions = quote
      [actions]
      var domain = [domain_from_bounds](
        [index_type:to_point(`([index_type](start_value)))],
        [index_type:to_point(`([index_type](extent_value)))])
      var [is] = c.legion_index_space_create_domain([cx.runtime], [cx.context], domain)
    end
  end

  actions = quote
    [actions]
    var [i] = [ispace_type]{ impl = [is] }
  end

  return values.value(expr.just(actions, i), ispace_type)
end

function codegen.expr_region(cx, node)
  local fspace_type = node.fspace_type
  local ispace = codegen.expr(cx, node.ispace):read(cx)
  local region_type = std.as_read(node.expr_type)
  local index_type = region_type:ispace().index_type
  local actions = quote
    [ispace.actions];
    [emit_debuginfo(node)]
  end

  local r = terralib.newsymbol(region_type, "r")
  local lr = terralib.newsymbol(c.legion_logical_region_t, "lr")
  local is = terralib.newsymbol(c.legion_index_space_t, "is")
  local fsa = terralib.newsymbol(c.legion_field_allocator_t, "fsa")
  local pr = terralib.newsymbol(c.legion_physical_region_t, "pr")

  local field_paths, field_types = std.flatten_struct_fields(fspace_type)
  local field_privileges = field_paths:map(function(_) return "reads_writes" end)
  local field_id = 100
  local field_ids = field_paths:map(
    function(_)
      field_id = field_id + 1
      return field_id
    end)
  local physical_regions = field_paths:map(function(_) return pr end)

  local pr_actions, base_pointers, strides = unpack(std.zip(unpack(
    std.zip(field_types, field_ids, field_privileges):map(
      function(field)
        local field_type, field_id, field_privilege = unpack(field)
        return terralib.newlist({
            physical_region_get_base_pointer(cx, index_type, field_type, field_id, field_privilege, pr)})
  end))))

  cx:add_region_root(region_type, r,
                     field_paths,
                     terralib.newlist({field_paths}),
                     std.dict(std.zip(field_paths:map(std.hash), field_privileges)),
                     std.dict(std.zip(field_paths:map(std.hash), field_types)),
                     std.dict(std.zip(field_paths:map(std.hash), field_ids)),
                     std.dict(std.zip(field_paths:map(std.hash), physical_regions)),
                     std.dict(std.zip(field_paths:map(std.hash), base_pointers)),
                     std.dict(std.zip(field_paths:map(std.hash), strides)))

  actions = quote
    [actions]
    var capacity = [ispace.value]
    var [is] = [ispace.value].impl
    var fs = c.legion_field_space_create([cx.runtime], [cx.context])
    var [fsa] = c.legion_field_allocator_create([cx.runtime], [cx.context],  fs);
    [std.zip(field_types, field_ids):map(
       function(field)
         local field_type, field_id = unpack(field)
         return `(c.legion_field_allocator_allocate_field(
                    [fsa], terralib.sizeof([field_type]), [field_id]))
       end)]
    var [lr] = c.legion_logical_region_create([cx.runtime], [cx.context], [is], fs)
    var il = c.legion_inline_launcher_create_logical_region(
      [lr], c.READ_WRITE, c.EXCLUSIVE, [lr], 0, false, 0, 0);
    [field_ids:map(
       function(field_id)
         return `(c.legion_inline_launcher_add_field(il, [field_id], true))
       end)]
    var [pr] = c.legion_inline_launcher_execute([cx.runtime], [cx.context], il)
    c.legion_inline_launcher_destroy(il)
    c.legion_physical_region_wait_until_valid([pr])
    [pr_actions]
    var [r] = [region_type]{ impl = [lr] }
  end

  return values.value(expr.just(actions, r), region_type)
end

function codegen.expr_partition(cx, node)
  local region_expr = codegen.expr(cx, node.region):read(cx)
  local coloring_expr = codegen.expr(cx, node.coloring):read(cx)
  local partition_type = std.as_read(node.expr_type)
  local actions = quote
    [region_expr.actions];
    [coloring_expr.actions];
    [emit_debuginfo(node)]
  end

  local index_partition_create
  local args = terralib.newlist({
      cx.runtime, cx.context,
      `([region_expr.value].impl.index_space),
  })
  if partition_type:parent_region():ispace().index_type:is_opaque() then
    index_partition_create = c.legion_index_partition_create_coloring
  else
    index_partition_create = c.legion_index_partition_create_domain_coloring
    local color_space = terralib.newsymbol()
    args:insert(color_space)
    actions = quote
      [actions]
      var [color_space] = c.legion_domain_coloring_get_color_space([coloring_expr.value])
    end
  end
  args:insertall({
      coloring_expr.value,
      node.disjointness == std.disjoint,
      -1,
  })

  local ip = terralib.newsymbol(c.legion_index_partition_t, "ip")
  local lp = terralib.newsymbol(c.legion_logical_partition_t, "lp")
  actions = quote
    [actions]
    var [ip] = [index_partition_create]([args])
    var [lp] = c.legion_logical_partition_create(
      [cx.runtime], [cx.context], [region_expr.value].impl, [ip])
  end

  return values.value(
    expr.once_only(actions, `(partition_type { impl = [lp] })),
    partition_type)
end

function codegen.expr_cross_product(cx, node)
  local args = node.args:map(function(arg) return codegen.expr(cx, arg):read(cx) end)
  local expr_type = std.as_read(node.expr_type)
  local actions = quote
    [args:map(function(arg) return arg.actions end)]
    [emit_debuginfo(node)]
  end

  local partitions = terralib.newsymbol(
    c.legion_index_partition_t[#args], "partitions")
  local product = terralib.newsymbol(
    c.legion_terra_index_cross_product_t, "cross_product")
  local lr = cx:region(expr_type:parent_region()).logical_region
  local lp = terralib.newsymbol(c.legion_logical_partition_t, "lp")
  actions = quote
    [actions]
    var [partitions]
    [std.zip(std.range(#args), args):map(
       function(pair)
         local i, arg = unpack(pair)
         return quote partitions[i] = [arg.value].impl.index_partition end
       end)]
    var [product] = c.legion_terra_index_cross_product_create_multi(
      [cx.runtime], [cx.context], &(partitions[0]), [#args])
    var ip = c.legion_terra_index_cross_product_get_partition([product])
    var [lp] = c.legion_logical_partition_create(
      [cx.runtime], [cx.context], lr.impl, ip)
  end

  return values.value(
    expr.once_only(
      actions,
      `(expr_type {
          impl = [lp],
          product = [product],
          partitions = [partitions],
        })),
    expr_type)
end

local lift_unary_op_to_futures = terralib.memoize(
  function (op, rhs_type, expr_type)
    assert(terralib.types.istype(rhs_type) and
             terralib.types.istype(expr_type))
    if std.is_future(rhs_type) then
      rhs_type = rhs_type.result_type
    end
    if std.is_future(expr_type) then
      expr_type = expr_type.result_type
    end

    local name = "__unary_" .. tostring(rhs_type) .. "_" .. tostring(op)
    local rhs_symbol = terralib.newsymbol(rhs_type, "rhs")
    local task = std.newtask(name)
    local node = ast.typed.StatTask {
      name = name,
      params = terralib.newlist({
          ast.typed.StatTaskParam {
            symbol = rhs_symbol,
            param_type = rhs_type,
            span = ast.trivial_span(),
          },
      }),
      return_type = expr_type,
      privileges = terralib.newlist(),
      constraints = terralib.newlist(),
      body = ast.typed.Block {
        stats = terralib.newlist({
            ast.typed.StatReturn {
              value = ast.typed.ExprUnary {
                op = op,
                rhs = ast.typed.ExprID {
                  value = rhs_symbol,
                  expr_type = rhs_type,
                  span = ast.trivial_span(),
                },
                expr_type = expr_type,
                span = ast.trivial_span(),
              },
              span = ast.trivial_span(),
            },
        }),
        span = ast.trivial_span(),
      },
      config_options = ast.typed.StatTaskConfigOptions {
        leaf = true,
        inner = false,
        idempotent = true,
      },
      region_divergence = false,
      prototype = task,
      inline = false,
      cuda = false,
      span = ast.trivial_span(),
    }
    task:settype(
      terralib.types.functype(
        node.params:map(function(param) return param.param_type end),
        node.return_type,
        false))
    task:setprivileges(node.privileges)
    task:set_param_constraints(node.constraints)
    task:set_constraints({})
    task:set_region_universe({})
    return codegen.entry(node)
  end)

local lift_binary_op_to_futures = terralib.memoize(
  function (op, lhs_type, rhs_type, expr_type)
    assert(terralib.types.istype(lhs_type) and
             terralib.types.istype(rhs_type) and
             terralib.types.istype(expr_type))
    if std.is_future(lhs_type) then
      lhs_type = lhs_type.result_type
    end
    if std.is_future(rhs_type) then
      rhs_type = rhs_type.result_type
    end
    if std.is_future(expr_type) then
      expr_type = expr_type.result_type
    end

    local name = ("__binary_" .. tostring(lhs_type) .. "_" ..
                    tostring(rhs_type) .. "_" .. tostring(op))
    local lhs_symbol = terralib.newsymbol(lhs_type, "lhs")
    local rhs_symbol = terralib.newsymbol(rhs_type, "rhs")
    local task = std.newtask(name)
    local node = ast.typed.StatTask {
      name = name,
      params = terralib.newlist({
         ast.typed.StatTaskParam {
            symbol = lhs_symbol,
            param_type = lhs_type,
            span = ast.trivial_span(),
         },
         ast.typed.StatTaskParam {
            symbol = rhs_symbol,
            param_type = rhs_type,
            span = ast.trivial_span(),
         },
      }),
      return_type = expr_type,
      privileges = terralib.newlist(),
      constraints = terralib.newlist(),
      body = ast.typed.Block {
        stats = terralib.newlist({
            ast.typed.StatReturn {
              value = ast.typed.ExprBinary {
                op = op,
                lhs = ast.typed.ExprID {
                  value = lhs_symbol,
                  expr_type = lhs_type,
                  span = ast.trivial_span(),
                },
                rhs = ast.typed.ExprID {
                  value = rhs_symbol,
                  expr_type = rhs_type,
                  span = ast.trivial_span(),
                },
                expr_type = expr_type,
                span = ast.trivial_span(),
              },
              span = ast.trivial_span(),
            },
        }),
        span = ast.trivial_span(),
      },
      config_options = ast.typed.StatTaskConfigOptions {
        leaf = true,
        inner = false,
        idempotent = true,
      },
      region_divergence = false,
      prototype = task,
      inline = false,
      cuda = false,
      span = ast.trivial_span(),
    }
    task:settype(
      terralib.types.functype(
        node.params:map(function(param) return param.param_type end),
        node.return_type,
        false))
    task:setprivileges(node.privileges)
    task:set_param_constraints(node.constraints)
    task:set_constraints({})
    task:set_region_universe({})
    return codegen.entry(node)
  end)

function codegen.expr_unary(cx, node)
  local expr_type = std.as_read(node.expr_type)
  if std.is_future(expr_type) then
    local rhs_type = std.as_read(node.rhs.expr_type)
    local task = lift_unary_op_to_futures(node.op, rhs_type, expr_type)

    local call = ast.typed.ExprCall {
      fn = ast.typed.ExprFunction {
        value = task,
        expr_type = task:gettype(),
        span = node.span,
      },
      inline = "allow",
      fn_unspecialized = false,
      args = terralib.newlist({node.rhs}),
      expr_type = expr_type,
      span = node.span,
    }
    return codegen.expr(cx, call)
  else
    local rhs = codegen.expr(cx, node.rhs):read(cx, expr_type)
    local actions = quote
      [rhs.actions];
      [emit_debuginfo(node)]
    end
    return values.value(
      expr.once_only(actions, std.quote_unary_op(node.op, rhs.value)),
      expr_type)
  end
end

function codegen.expr_binary(cx, node)
  local expr_type = std.as_read(node.expr_type)
  if std.is_future(expr_type) then
    local lhs_type = std.as_read(node.lhs.expr_type)
    local rhs_type = std.as_read(node.rhs.expr_type)
    local task = lift_binary_op_to_futures(
      node.op, lhs_type, rhs_type, expr_type)

    local call = ast.typed.ExprCall {
      fn = ast.typed.ExprFunction {
        value = task,
        expr_type = task:gettype(),
      span = node.span,
      },
      inline = "allow",
      fn_unspecialized = false,
      args = terralib.newlist({node.lhs, node.rhs}),
      expr_type = expr_type,
      span = node.span,
    }
    return codegen.expr(cx, call)
  else
    local lhs = codegen.expr(cx, node.lhs):read(cx, node.lhs.expr_type)
    local rhs = codegen.expr(cx, node.rhs):read(cx, node.rhs.expr_type)
    local actions = quote
      [lhs.actions];
      [rhs.actions];
      [emit_debuginfo(node)]
    end

    local expr_type = std.as_read(node.expr_type)
    return values.value(
      expr.once_only(actions, std.quote_binary_op(node.op, lhs.value, rhs.value)),
      expr_type)
  end
end

function codegen.expr_deref(cx, node)
  local value = codegen.expr(cx, node.value):read(cx)
  local value_type = std.as_read(node.value.expr_type)

  if value_type:ispointer() then
    return values.rawptr(value, value_type)
  elseif std.is_bounded_type(value_type) then
    assert(value_type:is_ptr())
    return values.ref(value, value_type)
  else
    assert(false)
  end
end

function codegen.expr_future(cx, node)
  local value = codegen.expr(cx, node.value):read(cx)
  local value_type = std.as_read(node.value.expr_type)
  local expr_type = std.as_read(node.expr_type)

  local actions = quote
    [value.actions];
    [emit_debuginfo(node)]
  end

  local result_type = std.type_size_bucket_type(value_type)
  if result_type == terralib.types.unit then
    assert(false)
  elseif result_type == c.legion_task_result_t then
    local result = terralib.newsymbol(c.legion_future_t, "result")
    local actions = quote
      [actions]
      var buffer = [value.value]
      var [result] = c.legion_future_from_buffer(
        [cx.runtime], [&opaque](&buffer), terralib.sizeof(value_type))
    end

    return values.value(
      expr.once_only(actions, `([expr_type]{ __result = [result] })),
      expr_type)
  else
    local result_type_name = std.type_size_bucket_name(result_type)
    local future_from_fn = c["legion_future_from" .. result_type_name]
    local result = terralib.newsymbol(c.legion_future_t, "result")
    local actions = quote
      [actions]
      var buffer = [value.value]
      var [result] = [future_from_fn]([cx.runtime], @[&result_type](&buffer))
    end
    return values.value(
      expr.once_only(actions, `([expr_type]{ __result = [result] })),
      expr_type)
  end
end

function codegen.expr_future_get_result(cx, node)
  local value = codegen.expr(cx, node.value):read(cx)
  local value_type = std.as_read(node.value.expr_type)
  local expr_type = std.as_read(node.expr_type)

  local actions = quote
    [value.actions];
    [emit_debuginfo(node)]
  end

  local result_type = std.type_size_bucket_type(expr_type)
  if result_type == terralib.types.unit then
    assert(false)
  elseif result_type == c.legion_task_result_t then
    local result_value = terralib.newsymbol(expr_type, "result_value")
    local expr_type_alignment = std.min(terralib.sizeof(expr_type), 8)
    local actions = quote
      [actions]
      var result = c.legion_future_get_result([value.value].__result)
        -- Force unaligned access because malloc does not provide
        -- blocks aligned for all purposes (e.g. SSE vectors).
      var [result_value] = terralib.attrload(
        [&expr_type](result.value),
        { align = [expr_type_alignment] })
      c.legion_task_result_destroy(result)
    end
    return values.value(
      expr.just(actions, result_value),
      expr_type)
  else
    local result_type_name = std.type_size_bucket_name(result_type)
    local get_result_fn = c["legion_future_get_result" .. result_type_name]
    local result_value = terralib.newsymbol(expr_type, "result_value")
    local actions = quote
      [actions]
      var result = [get_result_fn]([value.value].__result)
      var [result_value] = @[&expr_type](&result)
    end
    return values.value(
      expr.just(actions, result_value),
      expr_type)
  end
end

function codegen.expr(cx, node)
  if node:is(ast.typed.ExprInternal) then
    return codegen.expr_internal(cx, node)

  elseif node:is(ast.typed.ExprID) then
    return codegen.expr_id(cx, node)

  elseif node:is(ast.typed.ExprConstant) then
    return codegen.expr_constant(cx, node)

  elseif node:is(ast.typed.ExprFunction) then
    return codegen.expr_function(cx, node)

  elseif node:is(ast.typed.ExprFieldAccess) then
    return codegen.expr_field_access(cx, node)

  elseif node:is(ast.typed.ExprIndexAccess) then
    return codegen.expr_index_access(cx, node)

  elseif node:is(ast.typed.ExprMethodCall) then
    return codegen.expr_method_call(cx, node)

  elseif node:is(ast.typed.ExprCall) then
    return codegen.expr_call(cx, node)

  elseif node:is(ast.typed.ExprCast) then
    return codegen.expr_cast(cx, node)

  elseif node:is(ast.typed.ExprCtor) then
    return codegen.expr_ctor(cx, node)

  elseif node:is(ast.typed.ExprRawContext) then
    return codegen.expr_raw_context(cx, node)

  elseif node:is(ast.typed.ExprRawFields) then
    return codegen.expr_raw_fields(cx, node)

  elseif node:is(ast.typed.ExprRawPhysical) then
    return codegen.expr_raw_physical(cx, node)

  elseif node:is(ast.typed.ExprRawRuntime) then
    return codegen.expr_raw_runtime(cx, node)

  elseif node:is(ast.typed.ExprRawValue) then
    return codegen.expr_raw_value(cx, node)

  elseif node:is(ast.typed.ExprIsnull) then
    return codegen.expr_isnull(cx, node)

  elseif node:is(ast.typed.ExprNew) then
    return codegen.expr_new(cx, node)

  elseif node:is(ast.typed.ExprNull) then
    return codegen.expr_null(cx, node)

  elseif node:is(ast.typed.ExprDynamicCast) then
    return codegen.expr_dynamic_cast(cx, node)

  elseif node:is(ast.typed.ExprStaticCast) then
    return codegen.expr_static_cast(cx, node)

  elseif node:is(ast.typed.ExprIspace) then
    return codegen.expr_ispace(cx, node)

  elseif node:is(ast.typed.ExprRegion) then
    return codegen.expr_region(cx, node)

  elseif node:is(ast.typed.ExprPartition) then
    return codegen.expr_partition(cx, node)

  elseif node:is(ast.typed.ExprCrossProduct) then
    return codegen.expr_cross_product(cx, node)

  elseif node:is(ast.typed.ExprUnary) then
    return codegen.expr_unary(cx, node)

  elseif node:is(ast.typed.ExprBinary) then
    return codegen.expr_binary(cx, node)

  elseif node:is(ast.typed.ExprDeref) then
    return codegen.expr_deref(cx, node)

  elseif node:is(ast.typed.ExprFuture) then
    return codegen.expr_future(cx, node)

  elseif node:is(ast.typed.ExprFutureGetResult) then
    return codegen.expr_future_get_result(cx, node)

  else
    assert(false, "unexpected node type " .. tostring(node.node_type))
  end
end

function codegen.expr_list(cx, node)
  return node:map(function(item) return codegen.expr(cx, item) end)
end

function codegen.block(cx, node)
  return node.stats:map(
    function(stat) return codegen.stat(cx, stat) end)
end

function codegen.stat_if(cx, node)
  local clauses = terralib.newlist()

  -- Insert first clause in chain.
  local cond = codegen.expr(cx, node.cond):read(cx)
  local then_cx = cx:new_local_scope()
  local then_block = codegen.block(then_cx, node.then_block)
  clauses:insert({cond, then_block})

  -- Add rest of clauses.
  for _, elseif_block in ipairs(node.elseif_blocks) do
    local cond = codegen.expr(cx, elseif_block.cond):read(cx)
    local elseif_cx = cx:new_local_scope()
    local block = codegen.block(elseif_cx, elseif_block.block)
    clauses:insert({cond, block})
  end
  local else_cx = cx:new_local_scope()
  local else_block = codegen.block(else_cx, node.else_block)

  -- Build chain of clauses backwards.
  local tail = else_block
  repeat
    local cond, block = unpack(clauses:remove())
    tail = quote
      if [quote [cond.actions] in [cond.value] end] then
        [block]
      else
        [tail]
      end
    end
  until #clauses == 0
  return tail
end

function codegen.stat_while(cx, node)
  local cond = codegen.expr(cx, node.cond):read(cx)
  local body_cx = cx:new_local_scope()
  local block = codegen.block(body_cx, node.block)
  return quote
    while [quote [cond.actions] in [cond.value] end] do
      [block]
    end
  end
end

function codegen.stat_for_num(cx, node)
  local symbol = node.symbol
  local cx = cx:new_local_scope()
  local bounds = codegen.expr_list(cx, node.values):map(function(value) return value:read(cx) end)
  local cx = cx:new_local_scope()
  local block = codegen.block(cx, node.block)

  local v1, v2, v3 = unpack(bounds)
  if #bounds == 2 then
    return quote
      [v1.actions]; [v2.actions]
      for [symbol] = [v1.value], [v2.value] do
        [block]
      end
    end
  else
    return quote
      [v1.actions]; [v2.actions]; [v3.actions]
      for [symbol] = [v1.value], [v2.value], [v3.value] do
        [block]
      end
    end
  end
end

function codegen.stat_for_list(cx, node)
  local symbol = node.symbol
  local cx = cx:new_local_scope()
  local value = codegen.expr(cx, node.value):read(cx)
  local value_type = std.as_read(node.value.expr_type)
  local cx = cx:new_local_scope()
  local block = codegen.block(cx, node.block)

  local ispace_type, is, it
  if std.is_region(value_type) then
    ispace_type = value_type:ispace()
    assert(cx:has_ispace(ispace_type))
    is = `([value.value].impl.index_space)
    it = cx:ispace(ispace_type).index_iterator
  else
    ispace_type = value_type
    is = `([value.value].impl)
  end

  local actions = quote
    [value.actions]
  end
  local cleanup_actions = quote end

  local iterator_has_next, iterator_next_span -- For unstructured
  local domain -- For structured
  if ispace_type.dim == 0 then
    if it and cache_index_iterator then
      iterator_has_next = c.legion_terra_cached_index_iterator_has_next
      iterator_next_span = c.legion_terra_cached_index_iterator_next_span
      actions = quote
        [actions]
        c.legion_terra_cached_index_iterator_reset(it)
      end
    else
      iterator_has_next = c.legion_index_iterator_has_next
      iterator_next_span = c.legion_index_iterator_next_span
      it = terralib.newsymbol(c.legion_index_iterator_t, "it")
      actions = quote
        [actions]
        var [it] = c.legion_index_iterator_create([cx.runtime], [cx.context], [is])
      end
      cleanup_actions = quote
        c.legion_index_iterator_destroy([it])
      end
    end
  else
    domain = terralib.newsymbol(c.legion_domain_t, "domain")
    actions = quote
      [actions]
      var [domain] = c.legion_index_space_get_domain([cx.runtime], [cx.context], [is])
    end
  end

  if not cx.task_meta:getcuda() then
    if ispace_type.dim == 0 then
      return quote
        [actions]
        while iterator_has_next([it]) do
          var count : c.size_t = 0
          var base = iterator_next_span([it], &count, -1).value
          for i = 0, count do
            var [symbol] = [symbol.type]{
              __ptr = c.legion_ptr_t {
                value = base + i
              }
            }
            do
              [block]
            end
          end
        end
        [cleanup_actions]
      end
    else
      local fields = ispace_type.index_type.fields
      if fields then
        local domain_get_rect = c["legion_domain_get_rect_" .. tostring(ispace_type.dim) .. "d"]
        local rect = terralib.newsymbol("rect")
        local index = fields:map(function(field) return terralib.newsymbol(tostring(field)) end)
        local body = quote
          var [symbol] = [symbol.type] { __ptr = [symbol.type.index_type.impl_type]{ index } }
          do
            [block]
          end
        end
        for i = ispace_type.dim, 1, -1 do
          local rect_i = i - 1 -- C is zero-based, Lua is one-based
          body = quote
            for [ index[i] ] = rect.lo.x[rect_i], rect.hi.x[rect_i] + 1 do
              [body]
            end
          end
        end
        return quote
          [actions]
          var [rect] = [domain_get_rect]([domain])
          [body]
          [cleanup_actions]
        end
      else
        return quote
          [actions]
          var rect = c.legion_domain_get_rect_1d([domain])
          for i = rect.lo.x[0], rect.hi.x[0] + 1 do
            var [symbol] = [symbol.type]{ __ptr = i }
            do
              [block]
            end
          end
          [cleanup_actions]
        end
      end
    end
  else
    std.assert(std.config["cuda"],
      "cuda should be enabled to generate cuda kernels")
    std.assert(ispace_type.dim == 0 or not ispace_type.index_type.fields,
      "multi-dimensional index spaces are not supported yet")

    local cuda_opts = cx.task_meta:getcuda()
    -- wrap for-loop body as a terra function
    local N = cuda_opts.unrolling_factor
    local T = 256
    local threadIdX = cudalib.nvvm_read_ptx_sreg_tid_x
    local blockIdX = cudalib.nvvm_read_ptx_sreg_ctaid_x
    local blockDimX = cudalib.nvvm_read_ptx_sreg_ntid_x
    local base = terralib.newsymbol(uint32, "base")
    local count = terralib.newsymbol(c.size_t, "count")
    local tid = terralib.newsymbol(c.size_t, "tid")
    local ptr_init
    if ispace_type.dim == 0 then
      ptr_init = quote
        if [tid] >= [count] + [base] then return end
        var [symbol] = [symbol.type] {
          __ptr = c.legion_ptr_t {
            value = [tid]
          }
        }
      end
    else
      ptr_init = quote
        if [tid] >= [count] + [base] then return end
        var [symbol] = [symbol.type] { __ptr = [tid] }
      end
    end
    local function expr_codegen(expr) return codegen.expr(cx, expr):read(cx) end
    local undefined =
      traverse_symbols.find_undefined_symbols(expr_codegen, symbol, node.block)
    local args = terralib.newlist()
    for symbol, _ in pairs(undefined) do args:insert(symbol) end
    args:insert(base)
    args:insert(count)
    args:sort(function(s1, s2) return sizeof(s1.type) > sizeof(s2.type) end)

    local kernel_body = terralib.newlist()
    kernel_body:insert(quote
      var [tid] = [base] + (threadIdX() + [N] * blockIdX() * blockDimX())
    end)
    for i = 1, N do
      kernel_body:insert(quote
        [ptr_init];
        [block];
        [tid] = [tid] + [T]
      end)
    end

    local terra kernel([args])
      [kernel_body]
    end

    local task = cx.task_meta

    -- register the function for JIT compiling PTX
    local kernel_id = task:addcudakernel(kernel)

    -- kernel launch
    local kernel_call = cudahelper.codegen_kernel_call(kernel_id, count, args, N, T)

    if ispace_type.dim == 0 then
      return quote
        [actions]
        while iterator_has_next([it]) do
          var [count] : c.size_t = 0
          var [base] = iterator_next_span([it], &count, -1).value
          [kernel_call]
        end
        [cleanup_actions]
      end
    else
      return quote
        [actions]
        var rect = c.legion_domain_get_rect_1d([domain])
        var [count] = rect.hi.x[0] - rect.lo.x[0] + 1
        var [base] = rect.lo.x[0]
        [kernel_call]
        [cleanup_actions]
      end
    end
  end
end

function codegen.stat_for_list_vectorized(cx, node)
  if cx.task_meta:getcuda() then
    return codegen.stat_for_list(cx,
      ast.typed.StatForList {
        symbol = node.symbol,
        value = node.value,
        block = node.orig_block,
        vectorize = false,
        span = node.span,
      })
  end
  local symbol = node.symbol
  local cx = cx:new_local_scope()
  local value = codegen.expr(cx, node.value):read(cx)
  local value_type = std.as_read(node.value.expr_type)
  local cx = cx:new_local_scope()
  local block = codegen.block(cx, node.block)
  local orig_block = codegen.block(cx, node.orig_block)
  local vector_width = node.vector_width

  local ispace_type, is, it
  if std.is_region(value_type) then
    ispace_type = value_type:ispace()
    assert(cx:has_ispace(ispace_type))
    is = `([value.value].impl.index_space)
    it = cx:ispace(ispace_type).index_iterator
  else
    ispace_type = value_type
    is = `([value.value].impl)
  end

  local actions = quote
    [value.actions]
  end
  local cleanup_actions = quote end

  local iterator_has_next, iterator_next_span -- For unstructured
  local domain -- For structured
  if ispace_type.dim == 0 then
    if it and cache_index_iterator then
      iterator_has_next = c.legion_terra_cached_index_iterator_has_next
      iterator_next_span = c.legion_terra_cached_index_iterator_next_span
      actions = quote
        [actions]
        c.legion_terra_cached_index_iterator_reset(it)
      end
    else
      iterator_has_next = c.legion_index_iterator_has_next
      iterator_next_span = c.legion_index_iterator_next_span
      it = terralib.newsymbol(c.legion_index_iterator_t, "it")
      actions = quote
        [actions]
        var [it] = c.legion_index_iterator_create([cx.runtime], [cx.context], [is])
      end
      cleanup_actions = quote
        c.legion_index_iterator_destroy([it])
      end
    end
  else
    domain = terralib.newsymbol(c.legion_domain_t, "domain")
    actions = quote
      [actions]
      var [domain] = c.legion_index_space_get_domain([cx.runtime], [cx.context], [is])
    end
  end

  if ispace_type.dim == 0 then
    return quote
      [actions]
      while iterator_has_next([it]) do
        var count : c.size_t = 0
        var base = iterator_next_span([it], &count, -1).value
        var alignment : c.size_t = [vector_width]
        var start = (base + alignment - 1) and not (alignment - 1)
        var stop = (base + count) and not (alignment - 1)
        var final = base + count
        var i = base
        if count >= vector_width then
          while i < start do
            var [symbol] = [symbol.type]{ __ptr = c.legion_ptr_t { value = i }}
            do
              [orig_block]
            end
            i = i + 1
          end
          while i < stop do
            var [symbol] = [symbol.type]{ __ptr = c.legion_ptr_t { value = i }}
            do
              [block]
            end
            i = i + [vector_width]
          end
        end
        while i < final do
          var [symbol] = [symbol.type]{ __ptr = c.legion_ptr_t { value = i }}
          do
            [orig_block]
          end
          i = i + 1
        end
      end
      [cleanup_actions]
    end
  else
    local fields = ispace_type.index_type.fields
    if fields then
      -- XXX: multi-dimensional index spaces are not supported yet
      local domain_get_rect = c["legion_domain_get_rect_" .. tostring(ispace_type.dim) .. "d"]
      local rect = terralib.newsymbol("rect")
      local index = fields:map(function(field) return terralib.newsymbol(tostring(field)) end)
      local body = quote
        var [symbol] = [symbol.type] { __ptr = [symbol.type.index_type.impl_type]{ index } }
        do
          [block]
        end
      end
      for i = ispace_type.dim, 1, -1 do
        local rect_i = i - 1 -- C is zero-based, Lua is one-based
        body = quote
          for [ index[i] ] = rect.lo.x[rect_i], rect.hi.x[rect_i] + 1 do
            [orig_block]
          end
        end
      end
      return quote
        [actions]
        var [rect] = [domain_get_rect]([domain])
        [body]
        [cleanup_actions]
      end
    else
      return quote
        [actions]
        var rect = c.legion_domain_get_rect_1d([domain])
        var alignment = [vector_width]
        var base = rect.lo.x[0]
        var count = rect.hi.x[0] - rect.lo.x[0] + 1
        var start = (base + alignment - 1) and not (alignment - 1)
        var stop = (base + count) and not (alignment - 1)
        var final = base + count

        var i = base
        if count >= [vector_width] then
          while i < start do
            var [symbol] = [symbol.type]{ __ptr = i }
            do
              [orig_block]
            end
            i = i + 1
          end
          while i < stop do
            var [symbol] = [symbol.type]{ __ptr = i }
            do
              [block]
            end
            i = i + [vector_width]
          end
        end
        while i < final do
          var [symbol] = [symbol.type]{ __ptr = i }
          do
            [orig_block]
          end
          i = i + 1
        end
        [cleanup_actions]
      end
    end
  end
end

function codegen.stat_repeat(cx, node)
  local cx = cx:new_local_scope()
  local block = codegen.block(cx, node.block)
  local until_cond = codegen.expr(cx, node.until_cond):read(cx)
  return quote
    repeat
      [block]
    until [quote [until_cond.actions] in [until_cond.value] end]
  end
end

function codegen.stat_block(cx, node)
  local cx = cx:new_local_scope()
  return quote
    do
      [codegen.block(cx, node.block)]
    end
  end
end

function codegen.stat_index_launch(cx, node)
  local symbol = node.symbol
  local cx = cx:new_local_scope()
  local domain = codegen.expr_list(cx, node.domain):map(function(value) return value:read(cx) end)

  local fn = codegen.expr(cx, node.call.fn):read(cx)
  assert(std.is_task(fn.value))
  local args = terralib.newlist()
  local args_partitions = terralib.newlist()
  for i, arg in ipairs(node.call.args) do
    local partition = false
    if not node.args_provably.variant[i] then
      args:insert(codegen.expr(cx, arg):read(cx))
    else
      -- Run codegen halfway to get the partition. Note: Remember to
      -- splice the actions back in later.
      partition = codegen.expr(cx, arg.value):read(cx)

      -- Now run codegen the rest of the way to get the region.
      local partition_type = std.as_read(arg.value.expr_type)
      local region = codegen.expr(
        cx,
        ast.typed.ExprIndexAccess {
          value = ast.typed.ExprInternal {
            value = values.value(
              expr.just(quote end, partition.value),
              partition_type),
            expr_type = partition_type,
          },
          index = arg.index,
          expr_type = arg.expr_type,
          span = node.span,
        }):read(cx)
      args:insert(region)
    end
    args_partitions:insert(partition)
  end

  local actions = quote
    [domain[1].actions];
    [domain[2].actions];
    -- Ignore domain[3] because we know it is a constant.
    [fn.actions];
    [std.zip(args, args_partitions, node.args_provably.invariant):map(
       function(pair)
         local arg, arg_partition, invariant = unpack(pair)

         -- Here we slice partition actions back in.
         local arg_actions = quote end
         if arg_partition then
           arg_actions = quote [arg_actions]; [arg_partition.actions] end
         end

         -- Normal invariant arg actions.
         if invariant then
           arg_actions = quote [arg_actions]; [arg.actions] end
         end

         return arg_actions
       end)]
  end

  local arg_types = terralib.newlist()
  for i, arg in ipairs(args) do
    arg_types:insert(std.as_read(node.call.args[i].expr_type))
  end

  local arg_values = terralib.newlist()
  local param_types = node.call.fn.expr_type.parameters
  for i, arg in ipairs(args) do
    local arg_value = args[i].value
    if i <= #param_types and param_types[i] ~= std.untyped and
      not std.is_future(arg_types[i])
    then
      arg_values:insert(std.implicit_cast(arg_types[i], param_types[i], arg_value))
    else
      arg_values:insert(arg_value)
    end
  end

  local value_type = fn.value:gettype().returntype

  local params_struct_type = fn.value:get_params_struct()
  local task_args = terralib.newsymbol(params_struct_type)
  local task_args_setup = terralib.newlist()
  for i, arg in ipairs(args) do
    local invariant = node.args_provably.invariant[i]
    if not invariant then
      task_args_setup:insert(arg.actions)
    end
  end
  expr_call_setup_task_args(
    cx, fn.value, arg_values, arg_types, param_types,
    params_struct_type, fn.value:get_params_map(),
    task_args, task_args_setup)

  local launcher = terralib.newsymbol("launcher")

  -- Pass futures.
  local future_args_setup = terralib.newlist()
  for i, arg_type in ipairs(arg_types) do
    if std.is_future(arg_type) then
      local arg_value = arg_values[i]
      local param_type = param_types[i]
      expr_call_setup_future_arg(
        cx, fn.value, arg_value, arg_type, param_type,
        launcher, true, future_args_setup)
    end
  end

  -- Pass index spaces through index requirements.
  local ispace_args_setup = terralib.newlist()
  for i, arg_type in ipairs(arg_types) do
    if std.is_ispace(arg_type) then
      local param_type = param_types[i]

      if not node.args_provably.variant[i] then
        expr_call_setup_ispace_arg(
          cx, fn.value, arg_type, param_type, launcher, true, ispace_args_setup)
      else
        assert(false) -- FIXME: Implement index partitions

        -- local partition = args_partitions[i]
        -- assert(partition)
        -- expr_call_setup_ispace_partition_arg(
        --   cx, fn.value, arg_type, param_type, partition.value, launcher, true,
        --   ispace_args_setup)
      end
    end
  end

  -- Pass regions through region requirements.
  local region_args_setup = terralib.newlist()
  for _, i in ipairs(std.fn_param_regions_by_index(fn.value:gettype())) do
    local arg_type = arg_types[i]
    local param_type = param_types[i]

    if not node.args_provably.variant[i] then
      expr_call_setup_region_arg(
        cx, fn.value, arg_type, param_type, launcher, true, region_args_setup)
    else
      local partition = args_partitions[i]
      assert(partition)
      expr_call_setup_partition_arg(
        cx, fn.value, arg_type, param_type, partition.value, launcher, true,
        region_args_setup)
    end
  end

  local argument_map = terralib.newsymbol("argument_map")
  local launcher_setup = quote
    var [argument_map] = c.legion_argument_map_create()
    for [node.symbol] = [domain[1].value], [domain[2].value] do
      var [task_args]
      [task_args_setup]
      var t_args : c.legion_task_argument_t
      t_args.args = [&opaque](&[task_args])
      t_args.arglen = terralib.sizeof(params_struct_type)
      c.legion_argument_map_set_point(
        [argument_map],
        c.legion_domain_point_from_point_1d(
          c.legion_point_1d_t { x = arrayof(int32, [node.symbol]) }),
        t_args, true)
    end
    var g_args : c.legion_task_argument_t
    g_args.args = nil
    g_args.arglen = 0
    var [launcher] = c.legion_index_launcher_create(
      [fn.value:gettaskid()],
      c.legion_domain_from_rect_1d(
        c.legion_rect_1d_t {
          lo = c.legion_point_1d_t { x = arrayof(int32, [domain[1].value]) },
          hi = c.legion_point_1d_t { x = arrayof(int32, [domain[2].value] - 1) },
        }),
      g_args, [argument_map],
      c.legion_predicate_true(), false, 0, 0)
    [future_args_setup]
    [ispace_args_setup]
    [region_args_setup]
  end

  local execute_fn = c.legion_index_launcher_execute
  local execute_args = terralib.newlist({
      cx.runtime, cx.context, launcher})
  local reduce_as_type = std.as_read(node.call.expr_type)
  if std.is_future(reduce_as_type) then
    reduce_as_type = reduce_as_type.result_type
  end
  if node.reduce_lhs then
    execute_fn = c.legion_index_launcher_execute_reduction

    local op = std.reduction_op_ids[node.reduce_op][reduce_as_type]
    assert(op)
    execute_args:insert(op)
  end

  local future = terralib.newsymbol("future")
  local launcher_execute = quote
    var [future] = execute_fn(execute_args)
  end

  if node.reduce_lhs then
    local rhs_type = std.as_read(node.call.expr_type)
    local future_type = rhs_type
    if not std.is_future(rhs_type) then
      future_type = std.future(rhs_type)
    end

    local rh = terralib.newsymbol(future_type)
    local rhs = ast.typed.ExprInternal {
      value = values.value(expr.just(quote end, rh), future_type),
      expr_type = future_type,
    }

    if not std.is_future(rhs_type) then
      rhs = ast.typed.ExprFutureGetResult {
        value = rhs,
        expr_type = rhs_type,
        span = node.span,
      }
    end

    local reduce = ast.typed.StatReduce {
      op = node.reduce_op,
      lhs = terralib.newlist({node.reduce_lhs}),
      rhs = terralib.newlist({rhs}),
      span = node.span,
    }

    launcher_execute = quote
      [launcher_execute]
      var [rh] = [future_type]({ __result = [future] })
      [codegen.stat(cx, reduce)]
    end
  end

  local destroy_future_fn = c.legion_future_map_destroy
  if node.reduce_lhs then
    destroy_future_fn = c.legion_future_destroy
  end

  local launcher_cleanup = quote
    c.legion_argument_map_destroy([argument_map])
    destroy_future_fn([future])
    c.legion_index_launcher_destroy([launcher])
  end

  actions = quote
    [actions];
    [launcher_setup];
    [launcher_execute];
    [launcher_cleanup]
  end
  return actions
end

function codegen.stat_var(cx, node)
  local lhs = node.symbols
  local types = node.types
  local rhs = terralib.newlist()
  for i, value in pairs(node.values) do
    local rh = codegen.expr(cx, value)
    rhs:insert(rh:read(cx, value.expr_type))
  end

  local rhs_values = terralib.newlist()
  for i, rh in ipairs(rhs) do
    local rhs_type = std.as_read(node.values[i].expr_type)
    local lhs_type = types[i]
    if lhs_type then
      rhs_values:insert(std.implicit_cast(rhs_type, lhs_type, rh.value))
    else
      rhs_values:insert(rh.value)
    end
  end
  local actions = rhs:map(function(rh) return rh.actions end)

  if #rhs > 0 then
    local decls = terralib.newlist()
    for i, lh in ipairs(lhs) do
      if node.values[i]:is(ast.typed.ExprIspace) then
        actions = quote
          [actions]
          c.legion_index_space_attach_name([cx.runtime], [ rhs_values[i] ].impl, [lh.displayname])
        end
      elseif node.values[i]:is(ast.typed.ExprRegion) then
        actions = quote
          [actions]
          c.legion_logical_region_attach_name([cx.runtime], [ rhs_values[i] ].impl, [lh.displayname])
        end
      end
      decls:insert(quote var [lh] : types[i] = [ rhs_values[i] ] end)
    end
    return quote [actions]; [decls] end
  else
    local decls = terralib.newlist()
    for i, lh in ipairs(lhs) do
      decls:insert(quote var [lh] : types[i] end)
    end
    return quote [decls] end
  end
end

function codegen.stat_var_unpack(cx, node)
  local value = codegen.expr(cx, node.value):read(cx)
  local value_type = std.as_read(node.value.expr_type)

  local lhs = node.symbols
  local rhs = terralib.newlist()
  local actions = value.actions
  for i, field_name in ipairs(node.fields) do
    local field_type = node.field_types[i]

    local static_field_type = std.get_field(value_type, field_name)
    local field_value = std.implicit_cast(
      static_field_type, field_type, `([value.value].[field_name]))
    rhs:insert(field_value)

    if std.is_region(field_type) then
      local field_expr = expr.just(actions, field_value)
      field_expr = unpack_region(cx, field_expr, field_type, static_field_type)
      actions = quote [actions]; [field_expr.actions] end
    end
  end

  return quote
    [actions]
    var [lhs] = [rhs]
  end
end

function codegen.stat_return(cx, node)
  if not node.value then
    return quote return end
  end
  local value = codegen.expr(cx, node.value):read(cx)
  local value_type = std.as_read(node.value.expr_type)
  local return_type = cx.expected_return_type
  local result_type = std.type_size_bucket_type(return_type)

  local result = terralib.newsymbol("result")
  local actions = quote
    [value.actions]
    var [result] = [std.implicit_cast(value_type, return_type, value.value)]
  end

  if result_type == c.legion_task_result_t then
    return quote
      [actions]
      return c.legion_task_result_create(
        [&opaque](&[result]),
        terralib.sizeof([return_type]))
    end
  else
    return quote
      [actions]
      return @[&result_type](&[result])
    end
  end
end

function codegen.stat_break(cx, node)
  return quote break end
end

function codegen.stat_assignment(cx, node)
  local actions = terralib.newlist()
  local lhs = codegen.expr_list(cx, node.lhs)
  local rhs = codegen.expr_list(cx, node.rhs)
  rhs = std.zip(rhs, node.rhs):map(
    function(pair)
      local rh_value, rh_node = unpack(pair)
      local rh_expr = rh_value:read(cx, rh_node.expr_type)
      -- Capture the rhs value in a temporary so that it doesn't get
      -- overridden on assignment to the lhs (if lhs and rhs alias).
      rh_expr = expr.once_only(rh_expr.actions, rh_expr.value)
      actions:insert(rh_expr.actions)
      return values.value(
        expr.just(quote end, rh_expr.value),
        std.as_read(rh_node.expr_type))
    end)

  actions:insertall(
    std.zip(lhs, rhs, node.lhs):map(
      function(pair)
        local lh, rh, lh_node = unpack(pair)
        return lh:write(cx, rh, lh_node.expr_type).actions
      end))

  return quote [actions] end
end

function codegen.stat_reduce(cx, node)
  local actions = terralib.newlist()
  local lhs = codegen.expr_list(cx, node.lhs)
  local rhs = codegen.expr_list(cx, node.rhs)
  rhs = std.zip(rhs, node.rhs):map(
    function(pair)
      local rh_value, rh_node = unpack(pair)
      local rh_expr = rh_value:read(cx, rh_node.expr_type)
      actions:insert(rh_expr.actions)
      return values.value(
        expr.just(quote end, rh_expr.value),
        std.as_read(rh_node.expr_type))
    end)

  actions:insertall(
    std.zip(lhs, rhs, node.lhs):map(
      function(pair)
        local lh, rh, lh_node = unpack(pair)
        return lh:reduce(cx, rh, node.op, lh_node.expr_type).actions
      end))

  return quote [actions] end
end

function codegen.stat_expr(cx, node)
  local expr = codegen.expr(cx, node.expr):read(cx)
  return quote [expr.actions] end
end

function find_region_roots(cx, region_types)
  local roots_by_type = {}
  for _, region_type in ipairs(region_types) do
    assert(cx:has_region(region_type))
    local root_region_type = cx:region(region_type).root_region_type
    roots_by_type[root_region_type] = true
  end
  local roots = terralib.newlist()
  for region_type, _ in pairs(roots_by_type) do
    roots:insert(region_type)
  end
  return roots
end

function find_region_roots_physical(cx, region_types)
  local roots = find_region_roots(cx, region_types)
  local result = terralib.newlist()
  for _, region_type in ipairs(roots) do
    local physical_regions = cx:region(region_type).physical_regions
    local privilege_field_paths = cx:region(region_type).privilege_field_paths
    for _, field_paths in ipairs(privilege_field_paths) do
      for _, field_path in ipairs(field_paths) do
        result:insert(physical_regions[field_path:hash()])
      end
    end
  end
  return result
end

function codegen.stat_map_regions(cx, node)
  local roots = find_region_roots_physical(cx, node.region_types)
  local actions = terralib.newlist()
  for _, pr in ipairs(roots) do
    actions:insert(
      `(c.legion_runtime_remap_region([cx.runtime], [cx.context], [pr])))
  end
  for _, pr in ipairs(roots) do
    actions:insert(
      `(c.legion_physical_region_wait_until_valid([pr])))
  end
  return quote [actions] end
end

function codegen.stat_unmap_regions(cx, node)
  local roots = find_region_roots_physical(cx, node.region_types)
  local actions = terralib.newlist()
  for _, pr in ipairs(roots) do
    actions:insert(
      `(c.legion_runtime_unmap_region([cx.runtime], [cx.context], [pr])))
  end
  return quote [actions] end
end

function codegen.stat(cx, node)
  if node:is(ast.typed.StatIf) then
    return codegen.stat_if(cx, node)

  elseif node:is(ast.typed.StatWhile) then
    return codegen.stat_while(cx, node)

  elseif node:is(ast.typed.StatForNum) then
    return codegen.stat_for_num(cx, node)

  elseif node:is(ast.typed.StatForList) then
    return codegen.stat_for_list(cx, node)

  elseif node:is(ast.typed.StatForListVectorized) then
    return codegen.stat_for_list_vectorized(cx, node)

  elseif node:is(ast.typed.StatRepeat) then
    return codegen.stat_repeat(cx, node)

  elseif node:is(ast.typed.StatBlock) then
    return codegen.stat_block(cx, node)

  elseif node:is(ast.typed.StatIndexLaunch) then
    return codegen.stat_index_launch(cx, node)

  elseif node:is(ast.typed.StatVar) then
    return codegen.stat_var(cx, node)

  elseif node:is(ast.typed.StatVarUnpack) then
    return codegen.stat_var_unpack(cx, node)

  elseif node:is(ast.typed.StatReturn) then
    return codegen.stat_return(cx, node)

  elseif node:is(ast.typed.StatBreak) then
    return codegen.stat_break(cx, node)

  elseif node:is(ast.typed.StatAssignment) then
    return codegen.stat_assignment(cx, node)

  elseif node:is(ast.typed.StatReduce) then
    return codegen.stat_reduce(cx, node)

  elseif node:is(ast.typed.StatExpr) then
    return codegen.stat_expr(cx, node)

  elseif node:is(ast.typed.StatMapRegions) then
    return codegen.stat_map_regions(cx, node)

  elseif node:is(ast.typed.StatUnmapRegions) then
    return codegen.stat_unmap_regions(cx, node)

  else
    assert(false, "unexpected node type " .. tostring(node:type()))
  end
end

function get_params_map_type(params)
  if #params == 0 then
    return false
  elseif #params <= 64 then
    return uint64
  else
    assert(false)
  end
end

local function filter_fields(fields, privileges)
  local remove = terralib.newlist()
  for _, field in pairs(fields) do
    local privilege = privileges[std.hash(field)]
    if not privilege or std.is_reduction_op(privilege) then
      remove:insert(field)
    end
  end
  for _, field in ipairs(remove) do
    fields[field] = nil
  end
  return fields
end

function codegen.stat_task(cx, node)
  local task = node.prototype
  -- we temporaily turn off generating two task versions for cuda tasks
  if node.cuda then node.region_divergence = false end

  task:set_config_options(node.config_options)

  local params_struct_type = terralib.types.newstruct()
  params_struct_type.entries = terralib.newlist()
  task:set_params_struct(params_struct_type)

  -- The param map tracks which parameters are stored in the task
  -- arguments versus futures. The space in the params struct will be
  -- reserved either way, but this tells us where to find a valid copy
  -- of the data. Currently this field has a fixed size to keep the
  -- code here sane, though conceptually it's just a bit vector.
  local params_map_type = get_params_map_type(node.params)
  local params_map = false
  if params_map_type then
    params_map = terralib.newsymbol(params_map_type, "__map")
    params_struct_type.entries:insert(
      { field = params_map, type = params_map_type })
  end
  task:set_params_map_type(params_map_type)
  task:set_params_map(params_map)

  -- Normal arguments are straight out of the param types.
  params_struct_type.entries:insertall(node.params:map(
    function(param)
      return { field = param.symbol.displayname, type = param.param_type }
    end))

  -- Regions require some special handling here. Specifically, field
  -- IDs are going to be passed around dynamically, so we need to
  -- reserve some extra slots in the params struct here for those
  -- field IDs.
  local param_regions = std.fn_param_regions(task:gettype())
  local param_field_ids = terralib.newlist()
  for _, region in ipairs(param_regions) do
    local field_paths, field_types =
      std.flatten_struct_fields(region.fspace_type)
    local field_ids = field_paths:map(
      function(field_path)
        return terralib.newsymbol("field_" .. field_path:hash())
      end)
    param_field_ids:insertall(field_ids)
    params_struct_type.entries:insertall(
      std.zip(field_ids, field_types):map(
        function(field)
          local field_id, field_type = unpack(field)
          return { field = field_id, type = c.legion_field_id_t }
        end))
  end
  task:set_field_id_params(param_field_ids)

  local params = node.params:map(
    function(param) return param.symbol end)
  local param_types = task:gettype().parameters
  local return_type = node.return_type

  local c_task = terralib.newsymbol(c.legion_task_t, "task")
  local c_regions = terralib.newsymbol(&c.legion_physical_region_t, "regions")
  local c_num_regions = terralib.newsymbol(uint32, "num_regions")
  local c_context = terralib.newsymbol(c.legion_context_t, "context")
  local c_runtime = terralib.newsymbol(c.legion_runtime_t, "runtime")
  local c_params = terralib.newlist({
      c_task, c_regions, c_num_regions, c_context, c_runtime })

  local cx = cx:new_task_scope(return_type,
                               task:get_constraints(),
                               task:get_config_options().leaf,
                               task, c_task, c_context, c_runtime)

  -- Unpack the by-value parameters to the task.
  local task_args_setup = terralib.newlist()
  local args = terralib.newsymbol(&params_struct_type, "args")
  if #(task:get_params_struct():getentries()) > 0 then
    task_args_setup:insert(quote
      var [args]
      if c.legion_task_get_is_index_space(c_task) then
        var arglen = c.legion_task_get_local_arglen(c_task)
        std.assert(arglen == terralib.sizeof(params_struct_type),
                   ["arglen mismatch in " .. tostring(task.name) .. " (index task)"])
        args = [&params_struct_type](c.legion_task_get_local_args(c_task))
      else
        var arglen = c.legion_task_get_arglen(c_task)
        std.assert(arglen == terralib.sizeof(params_struct_type),
                   ["arglen mismatch " .. tostring(task.name) .. " (single task)"])
        args = [&params_struct_type](c.legion_task_get_args(c_task))
      end
    end)
    task_args_setup:insert(quote
      var [params_map] = args.[params_map]
    end)

    local future_count = terralib.newsymbol(int32, "future_count")
    local future_i = terralib.newsymbol(int32, "future_i")
    task_args_setup:insert(quote
      var [future_count] = c.legion_task_get_futures_size([c_task])
      var [future_i] = 0
    end)
    for i, param in ipairs(params) do
      local param_type = node.params[i].param_type
      local param_type_alignment = std.min(terralib.sizeof(param_type), 8)

      local future = terralib.newsymbol("future")
      local future_type = std.future(param_type)
      local future_result = codegen.expr(
        cx,
        ast.typed.ExprFutureGetResult {
          value = ast.typed.ExprInternal {
            value = values.value(
              expr.just(quote end, `([future_type]{ __result = [future] })),
              future_type),
            expr_type = future_type,
          },
          expr_type = param_type,
          span = node.span,
      }):read(cx)

      task_args_setup:insert(quote
        var [param] : param_type
        if ([params_map] and [2ULL ^ (i-1)]) == 0 then
          -- Force unaligned access because malloc does not provide
          -- blocks aligned for all purposes (e.g. SSE vectors).
          [param] = terralib.attrload(
            (&args.[param.displayname]),
            { align = [param_type_alignment] })
        else
          std.assert([future_i] < [future_count], "missing future in task param")
          var [future] = c.legion_task_get_future([c_task], [future_i])
          [future_result.actions]
          [param] = [future_result.value]
          [future_i] = [future_i] + 1
        end
      end)
    end
    task_args_setup:insert(quote
      std.assert([future_i] == [future_count], "extra futures left over in task params")
    end)
  end

  -- Prepare any region parameters to the task.

  -- Unpack field IDs passed by-value to the task.
  local param_field_ids = task:get_field_id_params()
  for _, param in ipairs(param_field_ids) do
    task_args_setup:insert(quote
      var [param] = args.[param]
    end)
  end

  -- Unpack the region requirements.
  local region_args_setup = terralib.newlist()
  do
    local physical_region_i = 0
    local param_field_id_i = 1
    for _, region_i in ipairs(std.fn_param_regions_by_index(task:gettype())) do
      local region_type = param_types[region_i]
      local index_type = region_type:ispace().index_type
      local r = params[region_i]
      local is = terralib.newsymbol(c.legion_index_space_t, "is")
      local isa = false
      if not cx.leaf then
        isa = terralib.newsymbol(c.legion_index_allocator_t, "isa")
      end
      local it = false
      if cache_index_iterator then
        it = terralib.newsymbol(c.legion_terra_cached_index_iterator_t, "it")
      end

      local privileges, privilege_field_paths, privilege_field_types =
        std.find_task_privileges(region_type, task:getprivileges())

      local privileges_by_field_path = std.group_task_privileges_by_field_path(
        privileges, privilege_field_paths)

      local field_paths, field_types =
        std.flatten_struct_fields(region_type.fspace_type)
      local field_ids_by_field_path = {}
      for _, field_path in ipairs(field_paths) do
        field_ids_by_field_path[field_path:hash()] = param_field_ids[param_field_id_i]
        param_field_id_i = param_field_id_i + 1
      end

      local physical_regions = terralib.newlist()
      local physical_regions_by_field_path = {}
      local physical_regions_index = terralib.newlist()
      local physical_region_actions = terralib.newlist()
      local base_pointers = terralib.newlist()
      local base_pointers_by_field_path = {}
      local strides = terralib.newlist()
      local strides_by_field_path = {}
      for i, field_paths in ipairs(privilege_field_paths) do
        local privilege = privileges[i]
        local field_types = privilege_field_types[i]
        local physical_region = terralib.newsymbol(
          c.legion_physical_region_t,
          "pr_" .. tostring(physical_region_i))

        physical_regions:insert(physical_region)
        physical_regions_index:insert(physical_region_i)
        physical_region_i = physical_region_i + 1

        if not task:get_config_options().inner then
          local pr_actions, pr_base_pointers, pr_strides = unpack(std.zip(unpack(
            std.zip(field_paths, field_types):map(
              function(field)
                local field_path, field_type = unpack(field)
                local field_id = field_ids_by_field_path[field_path:hash()]
                return terralib.newlist({
                    physical_region_get_base_pointer(cx, index_type, field_type, field_id, privilege, physical_region)})
          end))))

          physical_region_actions:insertall(pr_actions or {})
          base_pointers:insert(pr_base_pointers)

          for i, field_path in ipairs(field_paths) do
            physical_regions_by_field_path[field_path:hash()] = physical_region
            if privileges_by_field_path[field_path:hash()] ~= "none" then
              base_pointers_by_field_path[field_path:hash()] = pr_base_pointers[i]
              strides_by_field_path[field_path:hash()] = pr_strides[i]
            end
          end
        end
      end

      local actions = quote end

      if not cx.leaf then
        actions = quote
          [actions]
          var [is] = [r].impl.index_space
          var [isa] = c.legion_index_allocator_create([cx.runtime], [cx.context], [is])
        end
      end


      if cache_index_iterator then
        actions = quote
          [actions]
          var [it] = c.legion_terra_cached_index_iterator_create(
            [cx.runtime], [cx.context], [r].impl.index_space)
        end
      end

      region_args_setup:insert(actions)

      for i, field_paths in ipairs(privilege_field_paths) do
        local field_types = privilege_field_types[i]
        local privilege = privileges[i]
        local physical_region = physical_regions[i]
        local physical_region_index = physical_regions_index[i]

        region_args_setup:insert(quote
          var [physical_region] = [c_regions][ [physical_region_index] ]
        end)
      end
      region_args_setup:insertall(physical_region_actions)

      if not cx:has_ispace(region_type:ispace()) then
        cx:add_ispace_root(region_type:ispace(), is, isa, it)
      end
      cx:add_region_root(region_type, r,
                         field_paths,
                         privilege_field_paths,
                         privileges_by_field_path,
                         std.dict(std.zip(field_paths:map(std.hash), field_types)),
                         field_ids_by_field_path,
                         physical_regions_by_field_path,
                         base_pointers_by_field_path,
                         strides_by_field_path)
    end
  end

  local preamble = quote [emit_debuginfo(node)]; [task_args_setup]; [region_args_setup] end

  local body
  if node.region_divergence then
    local region_divergence = terralib.newlist()
    local cases
    local diagnostic = quote end
    for _, rs in pairs(node.region_divergence) do
      local r1 = rs[1]
      if cx:has_region(r1) then
        local contained = true
        local rs_cases
        local rs_diagnostic = quote end

        local r1_fields = cx:region(r1).field_paths
        local valid_fields = std.dict(std.zip(r1_fields, r1_fields))
        for _, r in ipairs(rs) do
          if not cx:has_region(r) then
            contained = false
            break
          end
          filter_fields(valid_fields, cx:region(r).field_privileges)
        end

        if contained then
          local r1_bases = cx:region(r1).base_pointers
          for _, r in ipairs(rs) do
            if r1 ~= r then
              local r_base = cx:region(r).base_pointers
              for field, _ in pairs(valid_fields) do
                local r1_base = r1_bases[field:hash()]
                local r_base = r_base[field:hash()]
                assert(r1_base and r_base)
                if rs_cases == nil then
                  rs_cases = `([r1_base] == [r_base])
                else
                  rs_cases = `([rs_cases] and [r1_base] == [r_base])
                  rs_diagnostic = quote
                    [rs_diagnostic]
                    c.printf(["comparing for divergence: regions %s %s field %s bases %p and %p\n"],
                      [tostring(r1)], [tostring(r)], [tostring(field)],
                      [r1_base], [r_base])
                  end
                end
              end
            end
          end

          local group = {}
          for _, r in ipairs(rs) do
            group[r] = true
          end
          region_divergence:insert({group = group, valid_fields = valid_fields})
          if cases == nil then
            cases = rs_cases
          else
            cases = `([cases] and [rs_cases])
          end
          diagnostic = quote
            [diagnostic]
            [rs_diagnostic]
          end
        end
      end
    end

    if cases then
      local div_cx = cx:new_local_scope()
      local body_div = codegen.block(div_cx, node.body)
      local check_div = quote end
      if dynamic_branches_assert then
        check_div = quote
          [diagnostic]
          std.assert(false, ["falling back to slow path in task " .. task.name .. "\n"])
        end
      end

      local nodiv_cx = cx:new_local_scope(region_divergence)
      local body_nodiv = codegen.block(nodiv_cx, node.body)

      body = quote
        if [cases] then
          [body_nodiv]
        else
          [check_div]
          [body_div]
        end
      end
    else
      body = codegen.block(cx, node.body)
    end
  else
    body = codegen.block(cx, node.body)
  end

  local proto = task:getdefinition()
  local result_type = std.type_size_bucket_type(return_type)
  terra proto([c_params]): result_type
    [preamble]; -- Semicolon required. This is not an array access.
    [body]
  end

  return task
end

function codegen.stat_fspace(cx, node)
  return node.fspace
end

function codegen.stat_top(cx, node)
  if node:is(ast.typed.StatTask) then
    if not node.cuda then
      local cpu_task = codegen.stat_task(cx, node)
      std.register_task(cpu_task)
      return cpu_task
    else
      local cuda_opts = node.cuda
      node.cuda = false
      local cpu_task = codegen.stat_task(cx, node)
      local cuda_task = cpu_task:make_variant()
      cuda_task:setcuda(cuda_opts)
      local new_node = node {
        cuda = cuda_opts,
        prototype = cuda_task,
      }
      cuda_task = codegen.stat_task(cx, new_node)
      std.register_task(cpu_task)
      std.register_task(cuda_task)
      return cpu_task
    end

  elseif node:is(ast.typed.StatFspace) then
    return codegen.stat_fspace(cx, node)

  else
    assert(false, "unexpected node type " .. tostring(node:type()))
  end
end

function codegen.entry(node)
  local cx = context.new_global_scope()
  return codegen.stat_top(cx, node)
end

return codegen
