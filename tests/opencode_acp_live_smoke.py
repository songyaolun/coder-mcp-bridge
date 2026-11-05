"""Opt-in real model/MCP validation: python -m tests.opencode_acp_live_smoke.
Requires the locally configured magpie/deepseek + deepseek-flash route.
Uses only disposable workspaces; does not change provider configuration.
"""
import sys,tempfile,json,time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from tests.test_mcp import McpClient

def connect():
 c=McpClient();c.request('initialize',{'protocolVersion':'2025-03-26','capabilities':{},'clientInfo':{'name':'opencode-acp-migration-live','version':'1'}})
 cfg=c.tool('agent-config',{'action':'set','backend':'opencode'});assert cfg['probe']['transport']=='acp-v1';return c

def terminal(c,r):
 while r['status'] not in ['completed','failed','cancelled','timed_out']:
  r=c.tool('agent-wait',{'runId':r['runId'],'afterRevision':r['revision'],'timeoutMs':10000,'resultChars':2000})
 return r
base={'timeout':90,'model':{'providerId':'magpie/deepseek','modelId':'deepseek-flash'},'thoughtLevel':'low'}
def main():
    c=connect()
    try:
     with tempfile.TemporaryDirectory(prefix='oc-migrate-a-') as a,tempfile.TemporaryDirectory(prefix='oc-migrate-b-') as b:
      runs=[]
      for cwd,word in [(a,'ACP_ALPHA'),(b,'ACP_BETA')]:
       r=c.tool('agent-start',{**base,'cwd':cwd,'prompt':f'用write文件工具创建当前工作区proof.txt，内容恰为{word}。只回答DONE。不要做其他事。'})
       runs.append((r,cwd,word))
      completed=[]
      for r,cwd,word in runs:
       r=terminal(c,r);print('WRITE',r['status'],r.get('result'),r.get('error'),r['usage'],flush=True)
       assert r['status']=='completed',r
       assert Path(cwd,'proof.txt').read_text().strip()==word
       completed.append(r)
      first=completed[0];tid=first['threadId']
      print('FORK',c.tool('agent-branch',{'runId':first['runId']}),flush=True)
      print('COMPACT',c.tool('agent-context',{'runId':first['runId'],'action':'compact'}),flush=True)
      for r in completed:c.tool('agent-close',{'runId':r['runId']})
      c.close();c=connect()
      r=terminal(c,c.tool('agent-start',{**base,'cwd':a,'threadId':tid,'prompt':'读取当前工作区proof.txt，只回答文件完整内容。'}))
      print('CROSS_PROCESS_RESUME',r['status'],r.get('result'),r.get('error'),flush=True)
      assert r['status']=='completed' and 'ACP_ALPHA' in r.get('result',''),r
      c.tool('agent-close',{'runId':r['runId']})
      Path(a,'seed.txt').write_text('READ_ONLY_SEED')
      r=terminal(c,c.tool('agent-start',{**base,'cwd':a,'workspaceAccess':'shared','prompt':'先read读取seed.txt，再用write尝试创建blocked.txt。若工具拒绝写入不要尝试其他方法。最后回答seed内容。'}))
      print('READ_ONLY',r['status'],r.get('result'),r.get('error'),flush=True)
      assert 'READ_ONLY_SEED' in r.get('result',''),r
      assert not Path(a,'blocked.txt').exists()
      c.tool('agent-close',{'runId':r['runId']})
      r=terminal(c,c.tool('agent-start',{**base,'cwd':a,'toolDenylist':['write'],'prompt':'仅用write工具尝试创建denied.txt。若拒绝不要尝试其他方法。'}))
      print('DENYLIST',r['status'],r.get('error'),flush=True);assert not Path(a,'denied.txt').exists()
      c.tool('agent-close',{'runId':r['runId']})
      r=c.tool('agent-start',{**base,'cwd':a,'prompt':'执行shell sleep 30，完成后回答FINISHED。'})
      while not r.get('threadId') and r['status'] not in ['failed','completed']:
       r=c.tool('agent-wait',{'runId':r['runId'],'afterRevision':r['revision'],'timeoutMs':10000})
      c.tool('agent-control',{'runId':r['runId'],'action':'cancel'});r=terminal(c,r)
      print('CANCEL',r['status'],flush=True);assert r['status']=='cancelled',r
      c.tool('agent-close',{'runId':r['runId']})
     print('OPENCODE ACP MCP LIVE PASS',flush=True)
    finally:c.close()


if __name__ == "__main__":
    main()
