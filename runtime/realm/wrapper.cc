// Written by Alexis Perry for use with Tapir-LLVM

#include "realm.h"
#include <set>

extern "C" {
  
  struct context {
    Realm::Runtime rt;
    //Realm::Machine m;
    //Realm::Processor proc;
    std::set<Realm::Event> events;
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
    //create and initialize the global context object
    //Realm::Runtime rt;
    //std::cout << "runtime object created" << std::endl;
    //rt.init(&argc, &argv);
    //std::cout << "Runtime initialized" << std::endl;

    _globalCTX = new context();
    std::cout << "context created" << std::endl;
    _globalCTX->rt = Realm::Runtime();
    std::cout << "runtime object created" << std::endl;
    _globalCTX->rt.init(&argc, &argv);
    std::cout << "Runtime initialized" << std::endl;
    
    //Realm::Machine m = Realm::Machine::get_machine();
    //std::cout << "machine object created" << std::endl;
    //tmp->m = m;
    //std::cout << "machine assigned to tmp" << std::endl;
    //tmp->proc = tmp->rt.impl->next_local_processor_id();
    //std::cout << "processor created" << std::endl;
    //std::set<Realm::Event> events {};
    //std::cout << "set of events created" << std::endl;
    //tmp->events = events;
    //std::cout << "events assigned to tmp" << std::endl;
    _globalCTX->cur_task = Realm::Processor::TASK_ID_FIRST_AVAILABLE;
    std::cout << "curtask set" << std::endl;
    
    //_globalCTX = (void *) tmp;
    //std::cout << "set _globalCTX to the new context - All Done" << std::endl;
    std::cout << "set _globalCTX - All Done" << std::endl;
    //context *ctx = (context*) getRealmCTX();
    //std::cout << "successfully got the realm context" << std::endl;

    return;
  }

  void realmSpawn(void (*func)(void), const void* args, size_t arglen, void* user_data, size_t user_data_len) {           
    /* take a function pointer to the task you want to run, 
       creates a CodeDescriptor from it
       needs pointer to user data and arguments (NULL for void?)
       needs size_t for len (0 for void?)
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

    Realm::ProfilingRequestSet prs;  //We don't care what it is for now, the default is fine
    std::cout << "Created a default ProfilingRequestSet" << std::endl;

    //get a processor to run on
    Realm::Machine::ProcessorQuery procquery(Realm::Machine::get_machine());
    Realm::Processor p = procquery.local_address_space().random();
    assert ( p != Realm::Processor::NO_PROC); //assert that the processor exists

    //register the task with the runtime
    Realm::Event e1 = p.register_task(taskID, cd, prs, user_data, user_data_len);
    std::cout << "Registered Task" << std::endl;
    ctx->events.insert(e1); //might not actually need to keep track of this one
    std::cout << "Added the register_task event to the context's set of Events" << std::endl;
    //spawn the task
    Realm::Event e2 = p.spawn(taskID, args, arglen, e1, 0); 
    std::cout << "Spawned the task" << std::endl;
    ctx->events.insert(e2);
    std::cout << "Added the spawn event to the context's set of Events" << std::endl;
    return;
  }
  
  void realmSync() {
    context *ctx = (context*) getRealmCTX();
    Realm::Event e;
    e = e.merge_events(ctx->events);
    ctx->events.clear();
    ctx->events.insert(e);
    return;
  }
}
