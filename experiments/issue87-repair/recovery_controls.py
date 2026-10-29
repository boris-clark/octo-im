#!/usr/bin/env python3
"""Prepare current-source recovery removal controls as Go overlays."""
import hashlib
import json
import pathlib
import sys

root = pathlib.Path.cwd().resolve()
out = pathlib.Path(sys.argv[1]).resolve()
out.mkdir(parents=True, exist_ok=True)
records = []


def control(name, filename, before, after, package, test):
    source = (root / filename).read_text()
    assert source.count(before) == 1, (name, "source no longer matches")
    target = out / (name + ".go")
    target.write_text(source.replace(before, after, 1))
    overlay = out / (name + ".json")
    overlay.write_text(json.dumps({"Replace": {str(root / filename): str(target)}}, indent=2) + "\n")
    records.append(dict(name=name, source=filename,
                        source_sha256=hashlib.sha256(source.encode()).hexdigest(),
                        control_sha256=hashlib.sha256(target.read_bytes()).hexdigest(),
                        package=package, test=test, overlay=str(overlay)))


control("without-current-term-entry", "pkg/raft/raft/node_read.go",
        "admission, err := n.Admit([]types.Log{entry})\n\treturn admission.LastIndex, err",
        "return n.LastLogIndex(), nil", "./pkg/raft/raft",
        "^TestRecoveryBarrierRequiresStoreAndDurableCommit$")
control("without-prefix-order", "pkg/cluster/channel/storage.go",
        "start := 0\n\tfor i, log := range logs {",
        """var laterMessages []wkdb.Message
	for i, log := range logs { if !types.IsNoop(log) { laterMessages = append(laterMessages,messages[i]) } }
	if len(laterMessages)>0 { if err := s.db.AppendMessages(channelId,channelType,laterMessages); err != nil {return err} }
	start := 0
	for i, log := range logs {""", "./pkg/cluster/channel",
        "^TestStorageRecoveryBarrierFailureKeepsDurablePrefix$")
control("with-store-panic", "pkg/raft/raftgroup/handle.go",
        'rg.Error("append logs failed", zap.Error(err))',
        'rg.Panic("append logs failed", zap.Error(err))', "./pkg/cluster/channel",
        "^TestConfigRecoveryBarrierFailuresContinuedWriteAndReopen/store$")
(out / "index.json").write_text(json.dumps(records, indent=2) + "\n")
print(out)
