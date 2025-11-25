import requests
import time
import os
from pathlib import Path

# PDF 생성을 위한 라이브러리
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.utils import simpleSplit

class AbstractToPDFCollector:
    
    BASE_URL = "https://api.semanticscholar.org/graph/v1"
    
    def __init__(self, data_dir: str = 'data'):
        # 현재 스크립트 위치 기준 data 폴더 생성
        script_dir = Path(__file__).resolve().parent
        self.pdf_dir = script_dir / data_dir
        self.pdf_dir.mkdir(parents=True, exist_ok=True)
        print(f"📂 PDF 저장 경로: {self.pdf_dir.absolute()}")
    
    def create_pdf(self, filename, title, authors, year, abstract, citations):
        """텍스트 정보를 받아 실제 PDF 파일로 생성하는 함수"""
        c = canvas.Canvas(str(filename), pagesize=letter)
        width, height = letter
        
        # 1. 제목 (Title)
        c.setFont("Helvetica-Bold", 16)
        # 긴 제목 줄바꿈 처리
        title_lines = simpleSplit(title, "Helvetica-Bold", 16, width - 100)
        y = height - 50
        for line in title_lines:
            c.drawString(50, y, line)
            y -= 20
            
        # 2. 메타데이터 (저자, 연도, 인용수)
        y -= 20
        c.setFont("Helvetica-Oblique", 12)
        meta_text = f"Year: {year} | Citations: {citations} | Authors: {authors}"
        c.drawString(50, y, meta_text)
        
        # 3. 구분선
        y -= 15
        c.line(50, y, width - 50, y)
        
        # 4. 초록 (Abstract) 본문
        y -= 30
        c.setFont("Helvetica-Bold", 14)
        c.drawString(50, y, "Abstract")
        
        y -= 20
        c.setFont("Helvetica", 11)
        
        # 초록 텍스트 줄바꿈 및 페이지 넘김 처리
        if abstract:
            abstract_lines = simpleSplit(abstract, "Helvetica", 11, width - 100)
            for line in abstract_lines:
                if y < 50: # 페이지 공간 부족하면 새 페이지
                    c.showPage()
                    c.setFont("Helvetica", 11)
                    y = height - 50
                c.drawString(50, y, line)
                y -= 14
        else:
            c.drawString(50, y, "(No Abstract Available)")
            
        c.save()

    def search_and_generate_pdfs(self, query: str, limit: int, min_citations: int):
        print(f"🔍 '{query}' 검색 시작 (목표: {limit}개, 인용수 {min_citations}회 이상)...")
        
        # 필요한 필드만 요청
        fields = ['paperId', 'title', 'year', 'authors', 'abstract', 'citationCount']
        
        saved_count = 0
        offset = 0
        
        while saved_count < limit:
            params = {
                'query': query,
                'limit': 100, 
                'offset': offset,
                'fields': ','.join(fields),
                'year': '2020-', 
                'sort': 'citationCount:desc'
            }
            
            print(f"\n📡 데이터 요청 중... (offset: {offset})")
            
            # API 요청 (재시도 로직)
            data = None
            for attempt in range(3):
                try:
                    res = requests.get(f"{self.BASE_URL}/paper/search", params=params, timeout=10)
                    res.raise_for_status()
                    data = res.json()
                    break
                except Exception as e:
                    print(f"⚠️ API 요청 지연 ({attempt+1}/3)...")
                    time.sleep(2)

            if not data:
                print("❌ 데이터 요청 실패")
                break

            papers = data.get('data', [])
            if not papers:
                print("⚠️ 더 이상 검색되는 논문이 없습니다.")
                break
            
            for paper in papers:
                if saved_count >= limit:
                    break
                
                # 1. 인용수 필터링
                citations = paper.get('citationCount', 0)
                if citations < min_citations:
                    continue
                
                # 2. 초록 존재 여부 확인 (초록이 있어야 PDF를 만듦)
                abstract = paper.get('abstract')
                if not abstract:
                    continue
                
                # 데이터 정제
                title = paper.get('title', 'Untitled')
                year = paper.get('year', 'Unknown')
                # 저자 이름 3명까지만 가져오기
                authors = ", ".join([a['name'] for a in paper.get('authors', [])[:3]]) 
                
                # 파일명 생성
                safe_title = "".join([c if c.isalnum() else "_" for c in title])[:50]
                filename = self.pdf_dir / f"{year}_{safe_title}.pdf"
                
                # PDF 생성 함수 호출
                self.create_pdf(filename, title, authors, year, abstract, citations)
                
                saved_count += 1
                print(f"✅ [{saved_count}/{limit}] PDF 생성 완료: {filename.name} (인용: {citations})")
            
            if saved_count >= limit:
                break
                
            offset += 100 # 다음 페이지
            
        print(f"\n🎉 총 {saved_count}개의 PDF 파일을 직접 생성했습니다.")

if __name__ == "__main__":
    collector = AbstractToPDFCollector()
    # 인용수 100회 이상인 논문 30개를 생성합니다.
    collector.search_and_generate_pdfs("Generative AI", limit=50, min_citations=100)