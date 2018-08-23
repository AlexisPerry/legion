// Written by Alexis Perry for use with Tapir-LLVM

#include "realm.h"
#include <set>
#include <typeinfo>
#include <typeindex>

extern "C" {
  
  struct context {
    Realm::Runtime rt;
    std::set<Realm::Event> events;
    std::set<Realm::Event> mem_events;
    unsigned cur_task;
  };

  static context *_globalCTX;  //global variable
  
  void * getRealmCTX() {
    std::cout << "start of getRealmCTX" << std::endl;
    if ( _globalCTX) {
      std::cout << "_globalCTX exists : returning it" << std::endl;
      return _globalCTX;
    }
    else {
      std::cout << "_globalCTX does not exist: returning a null pointer" << std::endl;
      return NULL;
    }
  }
  
  void realmInitRuntime(int argc, char** argv) {
    std::cout << "start of realmInitRuntime" << std::endl;

    _globalCTX = new context();
    std::cout << "context created" << std::endl;

    _globalCTX->rt = Realm::Runtime();
    std::cout << "runtime object created" << std::endl;
    _globalCTX->rt.init(&argc, &argv);
    std::cout << "Runtime initialized" << std::endl;
    
    _globalCTX->cur_task = Realm::Processor::TASK_ID_FIRST_AVAILABLE;
    std::cout << "curtask set" << std::endl;
    
    std::cout << "set _globalCTX - All Done" << std::endl;

    return;
  }


  //realmCreateRegion

  //only use this internally (or eliminate it all together and call destroy directly
  void realmDestroyRegion(void *region, void *event) {
    //region->destroy(*event);
    ((Realm::RegionInstance *)region)->destroy(*((Realm::Event *)event));
    return;
  }

#if 0

  //only use internally
  //Note: borrowed this routine from https://github.com/StanfordLegion/legion/blob/stable/examples/realm_stencil/realm_stencil.cc
  Realm::Event realmCopy(Realm::RegionInstance src_inst, 
			 Realm::RegionInstance dst_inst, 
			 Realm::FieldID fid,
			 Realm::Event wait_for) {
    Realm::CopySrcDstField src_field;
    src_field.inst = src_inst;
    src_field.field_id = fid;
    src_field.size = sizeof(DTYPE);

    std::vector<Realm::CopySrcDstField> src_fields;
    src_fields.push_back(src_field);

    Realm::CopySrcDstField dst_field;
    dst_field.inst = dst_inst;
    dst_field.field_id = fid;
    dst_field.size = sizeof(DTYPE);

    std::vector<Realm::CopySrcDstField> dst_fields;
    dst_fields.push_back(dst_field);

    return dst_inst.get_indexspace<2, coord_t>().copy(src_fields, dst_fields,
						      Realm::ProfilingRequestSet(),
						      wait_for);
  }
#endif
  void realmSpawn(void (*func)(void), 
		  const void* args, 
		  size_t arglen, 
		  void* user_data, 
		  size_t user_data_len, 
		  void* data_region) {           
    /* take a function pointer to the task you want to run, 
       creates a CodeDescriptor from it
       needs pointer to user data and arguments (NULL for void?)
       needs size_t for len (0 for void?)
       data_region is actually a pointer to a RegionInstance
     */
    std::cout << "start of realmSpawn" << std::endl;
    context *ctx = (context*) getRealmCTX();
    std::cout << "successfully got the realm context" << std::endl;

    //update current taskID
    ctx->cur_task++;
    std::cout << "updated cur_task" << std::endl;
    Realm::Processor::TaskFuncID taskID = ctx->cur_task;
    std::cout << "created the taskID" << std::endl;

    //takes fxn ptr, turns it into a TypeConv::from_cpp_type<TaskFuncPtr>()
    // the CodeDescriptor needs to be of that type
    // orig:     Realm::CodeDescriptor cd = Realm::CodeDescriptor(func);

    Realm::CodeDescriptor cd = Realm::CodeDescriptor(Realm::TypeConv::from_cpp_type<Realm::Processor::TaskFuncPtr>());
    std::cout << "Created CodeDescriptor" << std::endl;
    Realm::FunctionPointerImplementation fpi = Realm::FunctionPointerImplementation(func);
    std::cout << "Created FunctionPointerImplementation" << std::endl;
    cd.add_implementation(&fpi);
    std::cout << "added the implementation to the CodeDescriptor" << std::endl;
    //cd.add_implementation(Realm::FunctionPointerImplementation(func).clone());

    const Realm::ProfilingRequestSet prs;  //We don't care what it is for now, the default is fine
    std::cout << "Created a default ProfilingRequestSet" << std::endl;

    //get a processor to run on
    Realm::Machine::ProcessorQuery procquery(Realm::Machine::get_machine());
    Realm::Processor p = procquery.local_address_space().random();
    assert ( p != Realm::Processor::NO_PROC); //assert that the processor exists

    //get a memory associated with that processor to copy to
    Realm::Machine::MemoryQuery memquery(Realm::Machine::get_machine());
    Realm::Memory m = memquery.local_address_space().best_affinity_to(p).random();
    assert ( m != Realm::Memory::NO_MEMORY); //assert that the memory exists

    //create a physical region for the copy
    Realm::RegionInstance R;
    //constexpr auto user_data_type = std::type_index(DTYPE);
    //constexpr auto user_data_type = (constexpr)DTYPE.name();
    //Realm::InstanceLayout<user_data_len,typeid(user_data[0]).name()> il;
    //Realm::InstanceLayout<1,user_data_type> il = Realm::InstanceLayoutOpaque(user_data_len,alignof(user_data)); //alignment is what?
    //Realm::InstanceLayout<1,typeid(user_element).name()> il = Realm::InstanceLayoutOpaque(user_data_len,alignof(user_data)); //alignment is what?
    const Realm::InstanceLayoutGeneric * il = ((Realm::RegionInstance *)data_region)->get_layout(); //copy the layout of the source region

    Realm::Event regEvt = Realm::RegionInstance::create_instance(R,m,(Realm::InstanceLayoutGeneric *)il,prs, Realm::Event::NO_EVENT);
    ctx->events.insert(regEvt);
    
    //copy the user data to the region
    while (!regEvt.has_triggered())
      continue;
    R.write_untyped(0, user_data, user_data_len);

    //register the task with the runtime
    Realm::Event e1 = p.register_task(taskID, cd, prs, user_data, user_data_len);
    std::cout << "Registered Task" << std::endl;
    ctx->events.insert(e1); //might not actually need to keep track of this one
    std::cout << "Added the register_task event to the context's set of Events" << std::endl;

    //spawn the task
    Realm::Event e2 = p.spawn(taskID, args, arglen, regEvt, 0); //predicated on the creation of the region 
    std::cout << "Spawned the task" << std::endl;
    ctx->events.insert(e2);
    std::cout << "Added the spawn event to the context's set of Events" << std::endl;
    return;
  }
  
  void realmSync() {
    std::cout << "Start of realmSync" << std::endl;
    context *ctx = (context*) getRealmCTX();
    Realm::Event e;
    e = e.merge_events(ctx->events);
    std::cout << "merged events" << std::endl;

    ctx->events.clear();
    std::cout << "Cleared the context's set of events" << std::endl;
    ctx->events.insert(e);
    std::cout << "Added sync event to context's set of events" << std::endl;

    while (! e.has_triggered()) {
      std::cout << "Sync event NOT TRIGGERED" << std::endl;
      continue;
    }
    std::cout << "Sync event TRIGGERED" << std::endl;

    return;
  }
}
