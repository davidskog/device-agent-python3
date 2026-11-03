#!/usr/bin/env python3
"""Package only committed, public release inputs; never the working directory."""
import argparse
import ast
import gzip
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tarfile

FILES = ('davra_agent.py', 'davra_lib.py', 'davra_sdk.py', 'davra_setup.py',
         'install.sh', 'uninstall.sh', 'davra_agent.service', 'requirements.txt')


def git(*args):
    return subprocess.check_output(['git'] + list(args))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    if git('status', '--porcelain').strip():
        raise SystemExit('Release packaging requires a clean worktree')
    commit = git('rev-parse', 'HEAD').decode().strip()
    blobs = {'davra-agent/' + name: git('show', commit + ':davra-agent/' + name)
             for name in FILES}
    version = None
    for node in ast.parse(blobs['davra-agent/davra_lib.py'].decode()).body:
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'davraAgentVersion'
                                               for t in node.targets):
            version = ast.literal_eval(node.value)
    if version != 'TLS_1_3':
        raise SystemExit('Unexpected Agent version')
    manifest = {'version': version, 'repository': 'davidskog/device-agent-python3',
                'sourceGitSha': commit, 'files': [
                    {'path': path, 'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw)}
                    for path, raw in sorted(blobs.items())],
                'upgradePaths': ['davra-agent/davra_agent.py', 'davra-agent/davra_lib.py'],
                'configIncluded': False, 'dependenciesChanged': False}
    manifest_bytes = (json.dumps(manifest, indent=2, sort_keys=True) + '\n').encode()
    blobs['TLS_1_3-release.json'] = manifest_bytes
    archive = io.BytesIO()
    with gzip.GzipFile(filename='', mode='wb', fileobj=archive, mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode='w') as tar:
            for path, raw in sorted(blobs.items()):
                info = tarfile.TarInfo(path)
                info.size, info.mtime = len(raw), 0
                info.mode = 0o755 if path.endswith(('.py', '.sh')) else 0o644
                info.uid = info.gid = 0
                info.uname = info.gname = 'root'
                tar.addfile(info, io.BytesIO(raw))
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    name = 'davra-agent-TLS_1_3.tar.gz'
    for filename, raw in ((name, archive.getvalue()), ('TLS_1_3-release.json', manifest_bytes),
                          (name + '.sha256', (hashlib.sha256(archive.getvalue()).hexdigest() +
                                             '  ' + name + '\n').encode())):
        with (output / filename).open('xb') as stream:
            stream.write(raw)
    print(json.dumps({'version': version, 'sourceGitSha': commit,
                      'archive': str(output / name),
                      'archiveSha256': hashlib.sha256(archive.getvalue()).hexdigest()}))


if __name__ == '__main__':
    main()
