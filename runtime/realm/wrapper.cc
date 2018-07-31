// Written by Alexis Perry for use with Tapir-LLVM

#include "realm.h"
#include <set>

extern "C" {
  static void *_globalCTX;
  
  struct context {
    Realm::Runtime rt;
    Realm::Processor proc;
    std::set<Realm::Event> events;
    int cur_task;
  };
  
  void * getRealmCTX() {
    if ( _globalCTX) 
      return _globalCTX;
    else {
      context *tmp = new context();
      tmp->rt = Realm::Runtime(); 
      tmp->proc = Realm::Processor::NO_PROC;
      tmp->events = std::set<Realm::Event> {};
      tmp->cur_task = Realm::Processor::TASK_ID_FIRST_AVAILABLE;
      return (void *) tmp;
    }
  }

  void realmSpawn(void func(void *), const void* args, size_t arglen, void* user_data, size_t user_data_len) {           
    /* take a function pointer to the task you want to run, 
       creates a CodeDescriptor from it
       needs pointer to user data and arguments (unsure what to do with void)
       needs size_t for len (0 for void?)
     */

    context *ctx = (context*) getRealmCTX();

    //update current taskID
    ctx->cur_task++;
    int taskID = ctx->cur_task;

    Realm::CodeDescriptor cd = Realm::CodeDescriptor(func); //takes fxn ptr
    Realm::ProfilingRequestSet prs;

    //register the task with the runtime
    Realm::Event e1 = ctx->proc.register_task(taskID, cd, prs, user_data, user_data_len);
    ctx->events.insert(e1); //might not actually need to keep track of this one

    //spawn the task
    Realm::Event e2 = ctx->proc.spawn(taskID, args, arglen, e1); 

    ctx->events.insert(e2);
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
