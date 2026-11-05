"""Opt-in live DSH MCP smoke; runs real model/tool requests.
Run from repository root: .venv/bin/python -m tests.dsh_live_smoke
Requires the configured deepseek-account/deepseek-flash route.
"""
import sys,tempfile,json
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from tests.test_mcp import McpClient
def main():
    c=McpClient()
    def terminal(r):
     while r['status'] not in ['completed','failed','cancelled','timed_out']:
      r=c.tool('agent-wait',{'runId':r['runId'],'afterRevision':r['revision'],'timeoutMs':10000,'resultChars':1000})
     return r
    base={'timeout':90,'model':{'providerId':'deepseek-account','modelId':'deepseek-flash'},'thoughtLevel':'low'}
    try:
     c.request('initialize',{'protocolVersion':'2025-03-26','capabilities':{},'clientInfo':{'name':'dsh-live','version':'1'}})
     c.tool('agent-config',{'action':'set','backend':'dsh'})
     with tempfile.TemporaryDirectory(prefix='dsh-live-a-') as a,tempfile.TemporaryDirectory(prefix='dsh-live-b-') as b:
      runs=[]
      for cwd,word in [(a,'ALPHA_DSH'),(b,'BETA_DSH')]:
       r=c.tool('agent-start',{**base,'cwd':cwd,'prompt':f'用文件写入工具在当前工作区创建 proof.txt，内容恰好为 {word}。然后回答 DONE。不要做其他事。'})
       runs.append((r,cwd,word))
      print('CONCURRENT_STARTS',[r['runId'] for r,_,_ in runs],flush=True)
      for r,cwd,word in runs:
       r=terminal(r);print('WRITE',r['status'],r.get('result'),r['counts'],flush=True)
       assert r['status']=='completed',r
       assert Path(cwd,'proof.txt').read_text().strip()==word
       c.tool('agent-close',{'runId':r['runId']})
       if cwd==a: tid=r['threadId']
      r=terminal(c.tool('agent-start',{**base,'cwd':a,'threadId':tid,'prompt':'读取当前工作区的 proof.txt，只回答文件中的完整字符串。'}))
      assert r['status']=='completed' and 'ALPHA_DSH' in r.get('result',''),r
      print('RESUME',r['status'],r['threadId'],flush=True);c.tool('agent-close',{'runId':r['runId']})
      r=c.tool('agent-start',{**base,'cwd':a,'prompt':'运行bash sleep 30，结束后回答 FINISHED。'})
      while not r.get('threadId') and r['status'] not in ['failed','completed']:
       r=c.tool('agent-wait',{'runId':r['runId'],'afterRevision':r['revision'],'timeoutMs':10000})
      c.tool('agent-control',{'runId':r['runId'],'action':'cancel'})
      r=terminal(r);print('CANCEL',r['status'],flush=True);assert r['status']=='cancelled',r
      c.tool('agent-close',{'runId':r['runId']})
     print('DSH LIVE PASS',flush=True)
    finally:c.close()


if __name__ == "__main__":
    main()
