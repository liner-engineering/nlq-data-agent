#!/usr/bin/env python3
"""로그 분석 유틸리티 - 사용자 쿼리와 SQL 생성 과정을 추적합니다."""

import json
from pathlib import Path
from collections import defaultdict
from datetime import datetime

def analyze_logs(log_file="./logs/nlq-agent.log"):
    """로그 파일을 분석하여 사용자 쿼리와 SQL 생성 과정을 보여줍니다."""
    if not Path(log_file).exists():
        print(f"✗ 로그 파일 없음: {log_file}")
        return
    
    queries = []
    current_query = {}
    
    with open(log_file, 'r') as f:
        for line in f:
            try:
                entry = json.loads(line.strip())
            except json.JSONDecodeError:
                continue
            
            # 사용자 쿼리 추출
            if 'user_query' in entry:
                user_query = entry['user_query']
                
                if user_query != current_query.get('query'):
                    if current_query:
                        queries.append(current_query)
                    current_query = {
                        'query': user_query,
                        'start_time': entry['timestamp'],
                        'steps': []
                    }
                
                current_query['steps'].append({
                    'time': entry['timestamp'],
                    'level': entry['level'],
                    'logger': entry['logger'],
                    'message': entry['message'],
                    'duration_ms': entry.get('duration_ms')
                })
            
            # SQL 추출
            if 'sql' in entry:
                current_query['sql'] = entry['sql'][:200] + '...'
    
    if current_query:
        queries.append(current_query)
    
    # 결과 출력
    print("\n" + "="*80)
    print("로그 분석 결과")
    print("="*80)
    
    for i, q in enumerate(queries, 1):
        print(f"\n[쿼리 {i}] {q['query']}")
        print(f"시작: {q['start_time']}")
        
        if 'sql' in q:
            print(f"SQL: {q['sql']}")
        
        print("\n실행 단계:")
        for step in q['steps']:
            duration_str = f" ({step['duration_ms']:.0f}ms)" if step['duration_ms'] else ""
            print(f"  [{step['level']}] {step['message']}{duration_str}")
    
    print("\n" + "="*80)
    print(f"총 {len(queries)}개 쿼리 분석 완료")
    print("="*80)

if __name__ == "__main__":
    analyze_logs()
