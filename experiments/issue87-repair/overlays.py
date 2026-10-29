#!/usr/bin/env python3
"""Prepare pinned-source and one-change removal controls without editing source."""
import hashlib,json,pathlib,re,subprocess,sys
root=pathlib.Path.cwd().resolve()
out=pathlib.Path(sys.argv[1]).resolve();out.mkdir(parents=True,exist_ok=True)
base='bdfbce6951787f58706e1ce15f0cfb1bf02252df'
changed=subprocess.check_output(['git','diff','--name-only',base],text=True).splitlines()
new=subprocess.check_output(['git','ls-files','--others','--exclude-standard'],text=True).splitlines()
manifest={}
def write(name,mapping):
 replace={}
 for filename,content in mapping.items():
  target=out/name/filename;target.parent.mkdir(parents=True,exist_ok=True);target.write_text(content)
  replace[str(root/filename)]=str(target)
  manifest[str(target)]=hashlib.sha256(target.read_bytes()).hexdigest()
 path=out/(name+'.json');path.write_text(json.dumps({'Replace':replace},indent=2)+'\n');return path
baseline={}
keep={'pkg/raft/raft/issue87_stale_install_test.go','pkg/raft/raft/issue87_boundary_test.go'}
for filename in set(changed+new):
 if not filename.endswith('.go'):continue
 current=(root/filename).read_text()
 if filename in keep:
  baseline[filename]=current.replace('return types.StoreResponse(req, response)','return response')
 else:
  old=subprocess.run(['git','show',base+':'+filename],capture_output=True,text=True)
  baseline[filename]=old.stdout if old.returncode==0 else re.search(r'^package \w+',current,re.M)[0]+'\n'
write('baseline',baseline)
s=(root/'pkg/raft/raft/node_step.go').read_text()
s=s.replace('if e.Type == types.StoreResp {\n\t\tif !n.consumeStoreResponse(e) {\n\t\t\treturn nil\n\t\t}\n\t}', '')
write('without-store-auth',{'pkg/raft/raft/node_step.go':s})
s=(root/'pkg/raft/raftgroup/storage_lifetime.go').read_text().replace('if !op.effectsDone || !op.disposed {','if !op.effectsDone {')
write('without-terminal-drain',{'pkg/raft/raftgroup/storage_lifetime.go':s})
s=(root/'pkg/raft/raft/node.go').read_text().replace('if termStartIndexInfo != nil {\n\t\t\t\t\t\tlogs = logs[:i]\n\t\t\t\t\t\tbreak\n\t\t\t\t\t}', '').replace('for i, log := range logs {','for _, log := range logs {')
write('without-term-split',{'pkg/raft/raft/node.go':s})
s=(root/'pkg/raft/raft/node.go').read_text().replace('n.pendingStoreTermStartIndex = termStartIndexInfo','n.pendingStoreTermStartIndex = termStartIndexInfo\n            if termStartIndexInfo != nil { n.updateLastTermStartIndex(termStartIndexInfo.Term,termStartIndexInfo.Index) }')
write('without-durable-publication',{'pkg/raft/raft/node.go':s})
(out/'manifest.json').write_text(json.dumps({'base':base,'inputs':manifest},indent=2)+'\n')
print(out)
