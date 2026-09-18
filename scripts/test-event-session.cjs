const assert = require('node:assert/strict');
const fs = require('node:fs');
const ts = require('typescript');
const vm = require('node:vm');
const modules = new Map();
const callbacks = new Map();
class NativeEmitter {
 addListener(event, callback) { callbacks.set(event, callback); return {remove(){callbacks.delete(event)}}; }
}
function load(name) {
 if(modules.has(name)) return modules.get(name);
 const exports={}; modules.set(name,exports);
 const code=ts.transpileModule(fs.readFileSync(`src/${name}.ts`,'utf8'),{compilerOptions:{module:ts.ModuleKind.CommonJS}}).outputText;
 vm.runInNewContext(code,{exports,require(path){
  if(path==='react-native')return {NativeEventEmitter:NativeEmitter,NativeModules:{OpenIMSDKRN:{setEventSession(){}}},Platform:{select(){return ''}}};
  if(path==='./eventSession')return load('eventSession');
  throw Error(path);
 }});
 return exports;
}
const session=load('eventSession');
const {NativeOpenIMEmitter}=load('OpenIMSDK.native');
const seen=[];
NativeOpenIMEmitter.addListener('onSyncServerFinish', value=>seen.push(value));
session.setActiveEventSession('old');
const queued={eventSession:'old',data:true};
session.setActiveEventSession(null);
callbacks.get('onSyncServerFinish')(queued);
session.setActiveEventSession('new');
callbacks.get('onSyncServerFinish')(queued);
callbacks.get('onSyncServerFinish')({eventSession:'new',data:false});
assert.deepEqual(seen,[false]);
console.log('PASS: queued old-session events rejected; public payload unchanged');
