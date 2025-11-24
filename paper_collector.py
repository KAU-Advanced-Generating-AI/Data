import requests
import time
import json
from typing import List, Dict, Optional
from datetime import datetime
import hashlib
import os
from pathlib import Path

#데이터 수집 클래스
class SemanticScholarRAGCollector:
    
    BASE_URL = "https://api.semanticscholar.org/graph/v1"
    
    def __init__(self, api_key: Optional[str] = None, data_dir: str = '.data'):
        self.api_key = api_key
        self.headers = {}
        if api_key:
            self.headers['x-api-key'] = api_key
        
        # 스크립트 파일이 있는 디렉토리 기준으로 .data 폴더 생성
        script_dir = Path(__file__).parent  # 스크립트 파일의 부모 디렉토리
        self.data_dir = script_dir / data_dir
        self.data_dir.mkdir(exist_ok=True)
        print(f"데이터 저장 폴더: {self.data_dir.absolute()}")
    
    def search_papers_for_rag(
        self,
        query: str,
        min_citations: int = 100,
        total_limit: int = 500,
        year_from: Optional[int] = None
    ) -> List[Dict]:
        all_papers = []
        offset = 0
        batch_size = 100
        
        fields = [
            'paperId', 'title', 'abstract', 'year', 'authors',
            'citationCount', 'publicationDate', 'venue', 'url', 
            'externalIds', 'referenceCount', 'fieldsOfStudy',
            'publicationTypes', 'tldr'
        ]
        
        consecutive_failures = 0  # 연속 실패 카운트
        max_consecutive_failures = 3  # 최대 연속 실패 허용
        
        while len(all_papers) < total_limit:
            params = {
                'query': query,
                'offset': offset,
                'limit': batch_size,
                'fields': ','.join(fields),
            }
            
            if year_from:
                params['year'] = f'{year_from}-'
            
            url = f"{self.BASE_URL}/paper/search"
            
            # 재시도 로직
            max_retries = 3
            retry_delay = 5  # 초
            success = False
            
            for attempt in range(max_retries):
                try:
                    response = requests.get(url, params=params, headers=self.headers, timeout=30)
                    
                    # 429 Rate Limit 에러 처리
                    if response.status_code == 429:
                        wait_time = retry_delay * (2 ** attempt)  # 지수 백오프
                        print(f"Rate limit 도달. {wait_time}초 대기 중... (시도 {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                    
                    # 기타 HTTP 에러
                    response.raise_for_status()
                    data = response.json()
                    
                    papers = data.get('data', [])
                    if not papers:
                        print(f"더 이상 검색 결과가 없습니다. (offset: {offset})")
                        return sorted(
                            all_papers,
                            key=lambda x: (x.get('year') or 0, x.get('publicationDate') or ''),
                            reverse=True
                        )[:total_limit]
                    
                    # 인용수 필터링
                    filtered = [
                        p for p in papers 
                        if p.get('citationCount', 0) >= min_citations
                    ]
                    all_papers.extend(filtered)
                    
                    print(f"{len(filtered)}개 논문 수집 완료 (총 {len(all_papers)}개, offset: {offset})")
                    
                    offset += batch_size
                    consecutive_failures = 0  # 성공 시 실패 카운트 리셋
                    success = True
                    
                    # Rate limiting 방지 대기
                    if self.api_key:
                        time.sleep(1)  # API 키 있으면 1초
                    else:
                        time.sleep(3)  # API 키 없으면 3초
                    
                    if len(papers) < batch_size:
                        print(f"마지막 배치 도달 ({len(papers)}개)")
                        return sorted(
                            all_papers,
                            key=lambda x: (x.get('year') or 0, x.get('publicationDate') or ''),
                            reverse=True
                        )[:total_limit]
                    
                    break  # 성공하면 재시도 루프 탈출
                    
                except requests.exceptions.Timeout:
                    wait_time = retry_delay * (attempt + 1)
                    print(f"타임아웃 발생. {wait_time}초 후 재시도... (시도 {attempt + 1}/{max_retries})")
                    time.sleep(wait_time)
                    
                except requests.exceptions.RequestException as e:
                    wait_time = retry_delay * (attempt + 1)
                    if attempt < max_retries - 1:
                        print(f"요청 오류: {e}")
                        print(f"   {wait_time}초 후 재시도... (시도 {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                    else:
                        print(f"API 요청 최종 실패: {e}")
                        consecutive_failures += 1
            
            # 모든 재시도 실패
            if not success:
                consecutive_failures += 1
                print(f"배치 수집 실패 (연속 실패: {consecutive_failures}/{max_consecutive_failures})")
                
                if consecutive_failures >= max_consecutive_failures:
                    print(f"\n{max_consecutive_failures}회 연속 실패. 수집을 중단합니다.")
                    print(f"지금까지 수집한 {len(all_papers)}개의 논문을 반환합니다.")
                    break
                
                # 실패 후 더 긴 대기
                wait_time = 30
                print(f"{wait_time}초 대기 후 계속...")
                time.sleep(wait_time)
        
        # 최신순 정렬
        sorted_papers = sorted(
            all_papers,
            key=lambda x: (
                x.get('year') or 0,
                x.get('publicationDate') or ''
            ),
            reverse=True
        )
        
        return sorted_papers[:total_limit]
    
    # 문서 변환
    def prepare_rag_documents(self, papers: List[Dict]) -> List[Dict]:
        rag_documents = []
        
        for paper in papers:
            # 기본 문서 생성
            doc = self._create_rag_document(paper)
            rag_documents.append(doc)
        
        return rag_documents
    
    def _create_rag_document(self, paper: Dict) -> Dict:
        # 저자 정보 추출
        authors = paper.get('authors', [])
        author_names = [a.get('name', '') for a in authors if a.get('name')]
        
        # 요약 추출
        tldr = paper.get('tldr', {})
        summary = tldr.get('text', '') if tldr else ''
        
        # 초록 추출
        abstract = paper.get('abstract', '') or ''
        
        # 전체 텍스트 컨텐츠 생성 
        content_parts = []
        
        # 제목
        title = paper.get('title', '')
        if title:
            content_parts.append(f"Title: {title}")
        
        # 요약 (있는 경우)
        if summary:
            content_parts.append(f"Summary: {summary}")
        
        # 초록
        if abstract:
            content_parts.append(f"Abstract: {abstract}")
        
        # 연구 분야
        fields_of_study = paper.get('fieldsOfStudy', [])
        if fields_of_study:
            content_parts.append(f"Fields of Study: {', '.join(fields_of_study)}")
        
        full_content = "\n\n".join(content_parts)
        
        # 고유 ID 생성
        doc_id = paper.get('paperId') or hashlib.md5(title.encode()).hexdigest()
        
        # RAG 문서 구조
        rag_doc = {
            # 문서 식별
            'id': doc_id,
            'source': 'semantic_scholar',
            'type': 'academic_paper',
            
            # 검색용 텍스트 컨텐츠
            'content': full_content,
            
            # 메타데이터 (필터링 및 컨텍스트용)
            'metadata': {
                'title': title,
                'authors': author_names,
                'year': paper.get('year'),
                'publication_date': paper.get('publicationDate'),
                'venue': paper.get('venue', ''),
                'citation_count': paper.get('citationCount', 0),
                'fields_of_study': fields_of_study,
                'url': paper.get('url', ''),
                'doi': paper.get('externalIds', {}).get('DOI'),
                'arxiv_id': paper.get('externalIds', {}).get('ArXiv'),
            },
            
            # 원본 초록 (별도 보관)
            'abstract': abstract,
            'summary': summary,
        }
        
        return rag_doc
    
    def save_for_rag(
        self,
        papers: List[Dict],
        output_format: str = 'jsonl',  # 'jsonl', 'json', 'csv'
        output_path: str = 'papers_rag'
    ):
        rag_docs = self.prepare_rag_documents(papers)
        
        # .data 폴더 내 경로로 변경
        full_path = self.data_dir / output_path
        
        if output_format == 'jsonl':
            # JSONL 형식 (라인별 JSON)
            filepath = f'{full_path}.jsonl'
            with open(filepath, 'w', encoding='utf-8') as f:
                for doc in rag_docs:
                    f.write(json.dumps(doc, ensure_ascii=False) + '\n')
            print(f"{len(rag_docs)}개 문서가 {filepath}에 저장되었습니다.")
        
        elif output_format == 'json':
            # JSON 형식
            filepath = f'{full_path}.json'
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(rag_docs, f, ensure_ascii=False, indent=2)
            print(f"{len(rag_docs)}개 문서가 {filepath}에 저장되었습니다.")
        
        elif output_format == 'csv':
            # CSV 형식 (단순한 경우)
            import csv
            filepath = f'{full_path}.csv'
            with open(filepath, 'w', encoding='utf-8', newline='') as f:
                if rag_docs:
                    # 평면화된 필드만 저장
                    fieldnames = ['id', 'title', 'content', 'year', 'citation_count', 'url']
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    
                    for doc in rag_docs:
                        row = {
                            'id': doc['id'],
                            'title': doc['metadata']['title'],
                            'content': doc['content'][:500] + '...',  # 길이 제한
                            'year': doc['metadata']['year'],
                            'citation_count': doc['metadata']['citation_count'],
                            'url': doc['metadata']['url'],
                        }
                        writer.writerow(row)
            print(f"{len(rag_docs)}개 문서가 {filepath}에 저장되었습니다.")
        
        return rag_docs

# 벡터 DB에 적합한 형식
def create_vector_db_compatible_format(papers: List[Dict]) -> List[Dict]:
    documents = []
    
    for paper in papers:
        # 저자 정보
        authors = [a.get('name', '') for a in paper.get('authors', []) if a.get('name')]
        
        # 텍스트 컨텐츠
        title = paper.get('title', '')
        abstract = paper.get('abstract', '') or ''
        tldr = paper.get('tldr', {}).get('text', '') if paper.get('tldr') else ''
        
        # 컨텐츠 조합
        content = f"{title}\n\n{tldr}\n\n{abstract}".strip()
        
        doc = {
            'text': content,  # 임베딩할 텍스트
            'metadata': {
                'paper_id': paper.get('paperId'),
                'title': title,
                'authors': ', '.join(authors),
                'year': paper.get('year'),
                'citation_count': paper.get('citationCount', 0),
                'venue': paper.get('venue', ''),
                'url': paper.get('url', ''),
                'fields': paper.get('fieldsOfStudy', []),
            }
        }
        documents.append(doc)
    
    return documents

if __name__ == "__main__":
    # API 인스턴스 생성 (.data 폴더에 저장)
    collector = SemanticScholarRAGCollector(api_key=None, data_dir='.data')
    
    # 검색 쿼리
    search_query = "machine learning" 
    
    print(f"\n검색 중: '{search_query}'")
    print(f"조건: 인용수 100개 이상, 2020년 이후, 최신순 정렬")
    print(f"Rate Limit 방지를 위해 천천히 진행됩니다...\n")
    
    # 논문 수집 (50개로 변경)
    papers = collector.search_papers_for_rag(
        query=search_query,
        min_citations=100,
        total_limit=50,  # 100 → 50으로 변경
        year_from=2020
    )
    
    if len(papers) == 0:
        print("\n논문을 수집하지 못했습니다.")
        print("5-10분 후 다시 실행해보세요")
        exit(1)
    
    print(f"\n총 {len(papers)}개의 논문을 수집했습니다.\n")
    
    # RAG용 문서로 변환 및 저장
    print("RAG용 형식으로 저장 중...\n")
    
    # 1. JSONL 형식으로 저장
    collector.save_for_rag(papers, output_format='jsonl', output_path='papers_rag')
    
    # 2. JSON 형식으로도 저장
    collector.save_for_rag(papers, output_format='json', output_path='papers_rag')
    
    # 3. 벡터 DB용 형식으로 변환
    vector_docs = create_vector_db_compatible_format(papers)
    vector_filepath = collector.data_dir / 'papers_vector_db.json'
    with open(vector_filepath, 'w', encoding='utf-8') as f:
        json.dump(vector_docs, f, ensure_ascii=False, indent=2)
    print(f"벡터 DB용 문서가 {vector_filepath}에 저장되었습니다.")
    
    # 통계 출력
    print("\n" + "=" * 80)
    print("수집된 논문 통계")
    print("=" * 80)
    
    years = [p.get('year') for p in papers if p.get('year')]
    if years:
        print(f"연도 범위: {min(years)} - {max(years)}")
    
    citations = [p.get('citationCount', 0) for p in papers]
    if citations:
        print(f"평균 인용수: {sum(citations) / len(citations):.1f}")
        print(f"최대 인용수: {max(citations)}")
        print(f"최소 인용수: {min(citations)}")

    # 샘플 출력
    print("\n" + "=" * 80)
    print("첫 번째 논문 샘플 (RAG 형식)")
    print("=" * 80)
    
    if papers:
        rag_docs = collector.prepare_rag_documents([papers[0]])
        sample = rag_docs[0]
        print(f"\nID: {sample['id']}")
        print(f"Title: {sample['metadata']['title']}")
        print(f"Authors: {', '.join(sample['metadata']['authors'][:3])}")
        print(f"Year: {sample['metadata']['year']}")
        print(f"Citations: {sample['metadata']['citation_count']}")
        print(f"\nContent Preview:")
        print(sample['content'][:300] + "...")