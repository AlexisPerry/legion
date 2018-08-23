// Written by Alexis Perry for use with Tapir-LLVM

#include "realm.h"
#include <set>
//#include <array>
#include <vector>
//#include <typeinfo>
//#include <typeindex>

extern "C" {
  
  typedef struct context {
    Realm::Runtime rt;
    std::set<Realm::Event> events;
    std::set<Realm::Event> mem_events;
    unsigned cur_task;
    //Realm::ProfilingRequestSet prs; //the default profiling request set
  } context;

  static context *_globalCTX;  //global variable
  
  context * getRealmCTX() {
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
  void* realmCreateRegion_int(int* data) {

    context * ctx = getRealmCTX();

    const Realm::ProfilingRequestSet prs;  //We don't care what it is for now, the default is fine

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

    //create a point object out of the data being passed
    const size_t length = sizeof(data)/sizeof(data[0]);
    Realm::Point<length,int> pt = Realm::Point<length,int>(data);

    //create an indexspace out of the point
    std::vector<Realm::Point<length, int> > myPointVec;
    myPointVec.push_back(pt);
    const Realm::IndexSpace<length,int> is = Realm::IndexSpace<length,int>(myPointVec);

    //create a vector of field sizes
    std::vector<size_t> field_sizes = {sizeof(data[0])}; //data is an array of ints, so there is only one field

    //constexpr auto user_data_type = std::type_index(DTYPE);
    //constexpr auto user_data_type = (constexpr)DTYPE.name();
    //Realm::InstanceLayout<user_data_len,typeid(user_data[0]).name()> il;
    //Realm::InstanceLayout<1,user_data_type> il = Realm::InstanceLayoutOpaque(user_data_len,alignof(user_data)); //alignment is what?
    //Realm::InstanceLayout<1,typeid(user_element).name()> il = Realm::InstanceLayoutOpaque(user_data_len,alignof(user_data)); //alignment is what?
    //const Realm::InstanceLayoutGeneric * il = ((Realm::RegionInstance *)data_region)->get_layout(); //copy the layout of the source region

    //Realm::Event regEvt = Realm::RegionInstance::create_instance(R,m,(Realm::InstanceLayoutGeneric *)il,prs, Realm::Event::NO_EVENT);
    Realm::Event regEvt = Realm::RegionInstance::create_instance(R, m, is, field_sizes, 0, prs, Realm::Event::NO_EVENT); //the 0 denotes use SOA layout
    ctx->mem_events.insert(regEvt);

    return (void*) &R;
  }

  void realmDestroyRegion(void *region) {
    //region->destroy(*event);
    ((Realm::RegionInstance *)region)->destroy(Realm::Event::NO_EVENT); //destroys immediately
    return;
  }

  //only use this internally
  Realm::Event mem_sync();
  Realm::Event mem_sync() {
    context * ctx = getRealmCTX();
    Realm::Event e;
    e = e.merge_events(ctx->mem_events);
    std::cout << "merged memory events" << std::endl;

    ctx->mem_events.clear();
    std::cout << "Cleared the context's set of memory events" << std::endl;
    ctx->mem_events.insert(e);
    std::cout << "Added mem_sync event to context's set of memory events" << std::endl;

    return e;
  }


  //only use internally
  //Note: borrowed this routine from https://github.com/StanfordLegion/legion/blob/stable/examples/realm_stencil/realm_stencil.cc
  Realm::Event realmCopy_int(Realm::RegionInstance src_inst, 
			 Realm::RegionInstance dst_inst, 
			 Realm::FieldID fid, //int
			 Realm::Event wait_for) {
    Realm::CopySrcDstField src_field;
    src_field.inst = src_inst;
    src_field.field_id = fid;
    src_field.size = sizeof(int);

    std::vector<Realm::CopySrcDstField> src_fields;
    src_fields.push_back(src_field);

    Realm::CopySrcDstField dst_field;
    dst_field.inst = dst_inst;
    dst_field.field_id = fid;
    dst_field.size = sizeof(int);

    std::vector<Realm::CopySrcDstField> dst_fields;
    dst_fields.push_back(dst_field);

    return dst_inst.get_indexspace<2, long long int>().copy(src_fields, dst_fields,
						      Realm::ProfilingRequestSet(),
						      wait_for);
  }

  //only use internally
  //Note: borrowed this routine from https://github.com/StanfordLegion/legion/blob/stable/examples/realm_stencil/realm_stencil.cc
  Realm::Event realmCopy_double(Realm::RegionInstance src_inst, 
			 Realm::RegionInstance dst_inst, 
			 Realm::FieldID fid, //int
			 Realm::Event wait_for) {
    Realm::CopySrcDstField src_field;
    src_field.inst = src_inst;
    src_field.field_id = fid;
    src_field.size = sizeof(double);

    std::vector<Realm::CopySrcDstField> src_fields;
    src_fields.push_back(src_field);

    Realm::CopySrcDstField dst_field;
    dst_field.inst = dst_inst;
    dst_field.field_id = fid;
    dst_field.size = sizeof(double);

    std::vector<Realm::CopySrcDstField> dst_fields;
    dst_fields.push_back(dst_field);

    return dst_inst.get_indexspace<2, long long int>().copy(src_fields, dst_fields,
						      Realm::ProfilingRequestSet(),
						      wait_for);
  }

  //NOTE: this is for integers for now
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
    context *ctx = getRealmCTX();
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
    //predicate creation of this region on the creation and initialization of the old region
    Realm::Event mem_event = mem_sync();
    Realm::RegionInstance R;

    //constexpr auto user_data_type = std::type_index(DTYPE);
    //constexpr auto user_data_type = (constexpr)DTYPE.name();
    //Realm::InstanceLayout<user_data_len,typeid(user_data[0]).name()> il;
    //Realm::InstanceLayout<1,user_data_type> il = Realm::InstanceLayoutOpaque(user_data_len,alignof(user_data)); //alignment is what?
    //Realm::InstanceLayout<1,typeid(user_element).name()> il = Realm::InstanceLayoutOpaque(user_data_len,alignof(user_data)); //alignment is what?
    const Realm::InstanceLayoutGeneric * il = ((Realm::RegionInstance *)data_region)->get_layout(); //copy the layout of the source region

    //NOTE: the following is not implemented in realm, but only exists in a header file function declaration
    // If implemented, it would eliminate the need for ctx->mem_events.
    //Realm::Event regEvt = Realm::RegionInstance::create_instance(R,m,(Realm::InstanceLayoutGeneric *)il,prs, ((Realm::RegionInstance *)data_region)->get_ready_event());

    Realm::Event regEvt = Realm::RegionInstance::create_instance(R,m,(Realm::InstanceLayoutGeneric *)il,prs, mem_event);
    ctx->mem_events.insert(regEvt);
    
    //copy the user data to the region
    //while (!regEvt.has_triggered())
    //continue;
    //R.write_untyped(0, user_data, user_data_len);
    for(auto fieldPair : il->fields) {
      Realm::FieldID fid = fieldPair.first;
      Realm::Event copyEvt = realmCopy_int(*((Realm::RegionInstance *)data_region), R, fid, regEvt);
      ctx->mem_events.insert(copyEvt);
    }
    std::cout << "Finished copy" << std::endl;

    //register the task with the runtime
    Realm::Event e1 = p.register_task(taskID, cd, prs, user_data, user_data_len);
    std::cout << "Registered Task" << std::endl;
    ctx->events.insert(e1); //might not actually need to keep track of this one
    std::cout << "Added the register_task event to the context's set of Events" << std::endl;

    //spawn the task
    Realm::Event e2 = p.spawn(taskID, args, arglen, mem_sync(), 0); //predicated on the creation and initialization of the region 
    std::cout << "Spawned the task" << std::endl;
    ctx->events.insert(e2);
    std::cout << "Added the spawn event to the context's set of Events" << std::endl;

    //copy the data back over
    for(auto fieldPair : il->fields) {
      Realm::FieldID fid = fieldPair.first;
      Realm::Event copyBackEvt = realmCopy_int(R,*((Realm::RegionInstance *)data_region), fid, e2); //predicated on completion of spawned task
      ctx->mem_events.insert(copyBackEvt);
    }

    //free the newly created region after copy back finishes
    Realm::Event allDone = mem_sync();
    while (! allDone.has_triggered()) {
      std::cout << "realmSpawn allDone event has NOT TRIGGERED" << std::endl;
      continue;
    }
    realmDestroyRegion((void*) &R);

    return;
  }
  
  void realmSync() {
    std::cout << "Start of realmSync" << std::endl;
    context *ctx = getRealmCTX();
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
