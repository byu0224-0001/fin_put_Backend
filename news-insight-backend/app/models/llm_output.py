# -*- coding: utf-8 -*-
"""
LLM 출력 데이터 검증을 위한 Pydantic 모델

High Priority:
- 숫자 필드 타입 변환 (BIS 비율, 비율 등)
- 리스트 필드 검증 (major_products, keywords)
"""
from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field, field_validator, model_validator
import re
import logging

logger = logging.getLogger(__name__)


def parse_percentage(value: Any) -> Optional[float]:
    """퍼센트 문자열을 숫자로 변환 (예: "12.5%" -> 12.5)"""
    if value is None:
        return None
    
    if isinstance(value, (int, float)):
        return float(value)
    
    if isinstance(value, str):
        # "12.5%", "12.5 %", "12.5퍼센트" 등 처리
        cleaned = value.strip().replace('%', '').replace('퍼센트', '').replace('percent', '').strip()
        try:
            return float(cleaned)
        except (ValueError, AttributeError):
            logger.warning(f"퍼센트 파싱 실패: {value}")
            return None
    
    return None


def parse_number(value: Any) -> Optional[float]:
    """숫자 문자열을 숫자로 변환 (쉼표, 한글 제거)"""
    if value is None:
        return None
    
    if isinstance(value, (int, float)):
        return float(value)
    
    if isinstance(value, str):
        # "100억 원", "10,000", "10만" 등 처리
        cleaned = value.strip()
        
        # 한글 단위 제거 및 변환
        if '억' in cleaned:
            multiplier = 100000000
            cleaned = cleaned.replace('억', '').strip()
        elif '만' in cleaned:
            multiplier = 10000
            cleaned = cleaned.replace('만', '').strip()
        else:
            multiplier = 1
        
        # 쉼표, 공백, "원" 등 제거
        cleaned = cleaned.replace(',', '').replace('원', '').replace(' ', '').strip()
        
        try:
            return float(cleaned) * multiplier
        except (ValueError, AttributeError):
            logger.warning(f"숫자 파싱 실패: {value}")
            return None
    
    return None


def clean_string_list(value: Any) -> List[str]:
    """리스트 필드 검증 및 정리"""
    if value is None:
        return []
    
    if isinstance(value, str):
        # 문자열이면 리스트로 변환 시도
        if value.strip() == "" or value.lower() in ["정보없음", "none", "null"]:
            return []
        # 쉼표로 구분된 문자열 처리
        return [item.strip() for item in value.split(',') if item.strip()]
    
    if isinstance(value, list):
        # 리스트인 경우 각 항목 검증
        cleaned = []
        for item in value:
            if item is None:
                continue
            item_str = str(item).strip()
            if item_str and item_str.lower() not in ["정보없음", "none", "null", ""]:
                cleaned.append(item_str)
        return cleaned
    
    return []


class CapitalAdequacy(BaseModel):
    """자본적정성 지표"""
    bis_total_ratio: Optional[float] = Field(None, description="BIS 총자본비율 (%)")
    bis_tier1_ratio: Optional[float] = Field(None, description="BIS 기본자본비율 (%)")
    bis_cet1_ratio: Optional[float] = Field(None, description="BIS 보통주자본비율 (%)")
    total_capital: Optional[float] = Field(None, description="총자본 (억원)")
    risk_weighted_assets: Optional[float] = Field(None, description="위험가중자산 (억원)")
    report_year: Optional[str] = Field(None, description="보고서 기준연도")
    
    @field_validator('bis_total_ratio', 'bis_tier1_ratio', 'bis_cet1_ratio', mode='before')
    @classmethod
    def parse_percentage_field(cls, v):
        return parse_percentage(v)
    
    @field_validator('total_capital', 'risk_weighted_assets', mode='before')
    @classmethod
    def parse_number_field(cls, v):
        return parse_number(v)


class RevenueStructure(BaseModel):
    """수익 구조"""
    interest_income_ratio: Optional[float] = Field(None, description="이자수익 비중 (%)")
    fee_income_ratio: Optional[float] = Field(None, description="수수료수익 비중 (%)")
    trading_income_ratio: Optional[float] = Field(None, description="트레이딩수익 비중 (%)")
    
    @field_validator('interest_income_ratio', 'fee_income_ratio', 'trading_income_ratio', mode='before')
    @classmethod
    def parse_percentage_field(cls, v):
        return parse_percentage(v)


class LoansStructure(BaseModel):
    """대출 구성"""
    corporate: Optional[float] = Field(None, description="기업대출 비중 (%)")
    retail: Optional[float] = Field(None, description="가계대출 비중 (%)")
    mortgage: Optional[float] = Field(None, description="주택담보대출 비중 (%)")
    
    @field_validator('corporate', 'retail', 'mortgage', mode='before')
    @classmethod
    def parse_percentage_field(cls, v):
        return parse_percentage(v)


class SecuritiesStructure(BaseModel):
    """유가증권 구성"""
    bonds: Optional[float] = Field(None, description="채권 비중 (%)")
    stocks: Optional[float] = Field(None, description="주식 비중 (%)")
    alternatives: Optional[float] = Field(None, description="대체투자 비중 (%)")
    
    @field_validator('bonds', 'stocks', 'alternatives', mode='before')
    @classmethod
    def parse_percentage_field(cls, v):
        return parse_percentage(v)


class FundingStructure(BaseModel):
    """자금 조달 구조"""
    sources: List[str] = Field(default_factory=list, description="자금 조달원 리스트")
    cost_of_funding: Optional[float] = Field(None, description="이자비용률")
    rate_sensitivity: Optional[str] = Field(None, description="금리 민감도")
    duration_structure: Optional[str] = Field(None, description="ALM 구조")
    
    @field_validator('sources', mode='before')
    @classmethod
    def clean_sources(cls, v):
        return clean_string_list(v)
    
    @field_validator('cost_of_funding', mode='before')
    @classmethod
    def parse_percentage_field(cls, v):
        return parse_percentage(v)


class AssetStructure(BaseModel):
    """자산 구성"""
    loans: Optional[LoansStructure] = None
    securities: Optional[SecuritiesStructure] = None
    industry_exposure: List[str] = Field(default_factory=list, description="산업군 노출 리스트")
    
    @field_validator('industry_exposure', mode='before')
    @classmethod
    def clean_industry_exposure(cls, v):
        return clean_string_list(v)


class CreditRisk(BaseModel):
    """신용리스크"""
    npl_ratio: Optional[float] = Field(None, description="부실채권 비율 (%)")
    provision_ratio: Optional[float] = Field(None, description="충당금 비율 (%)")
    stage3_ratio: Optional[float] = Field(None, description="Stage3 비율 (%)")
    
    @field_validator('npl_ratio', 'provision_ratio', 'stage3_ratio', mode='before')
    @classmethod
    def parse_percentage_field(cls, v):
        return parse_percentage(v)


class MarketRisk(BaseModel):
    """시장리스크"""
    rate_risk: Optional[str] = Field(None, description="금리 리스크")
    fx_risk: Optional[str] = Field(None, description="환율 리스크")
    equity_risk: Optional[str] = Field(None, description="주가 리스크")


class LiquidityRisk(BaseModel):
    """유동성리스크"""
    lcr: Optional[float] = Field(None, description="유동성커버리지비율")
    loan_to_deposit_ratio: Optional[float] = Field(None, description="예대율")
    nsfr: Optional[float] = Field(None, description="순안정자금비율")
    
    @field_validator('lcr', 'loan_to_deposit_ratio', 'nsfr', mode='before')
    @classmethod
    def parse_percentage_field(cls, v):
        return parse_percentage(v)


class RiskExposure(BaseModel):
    """리스크 노출"""
    credit_risk: Optional[CreditRisk] = None
    market_risk: Optional[MarketRisk] = None
    liquidity_risk: Optional[LiquidityRisk] = None
    sector_exposure: List[str] = Field(default_factory=list, description="특정 섹터 노출 리스트")
    
    @field_validator('sector_exposure', mode='before')
    @classmethod
    def clean_sector_exposure(cls, v):
        return clean_string_list(v)


class FinancialValueChain(BaseModel):
    """금융사 밸류체인"""
    funding_structure: Optional[FundingStructure] = None
    asset_structure: Optional[AssetStructure] = None
    revenue_structure: Optional[RevenueStructure] = None
    capital_adequacy: Optional[CapitalAdequacy] = None
    risk_exposure: Optional[RiskExposure] = None
    major_counterparties: List[str] = Field(default_factory=list, description="주요 거래 상대방")
    
    @field_validator('major_counterparties', mode='before')
    @classmethod
    def clean_major_counterparties(cls, v):
        return clean_string_list(v)


class SupplyChainItem(BaseModel):
    """공급망 항목"""
    item: str = Field(..., description="원재료명")
    supplier: str = Field(..., description="공급사명")
    
    @field_validator('item', 'supplier', mode='before')
    @classmethod
    def clean_string_field(cls, v):
        if v is None:
            return "정보없음"
        return str(v).strip() if str(v).strip() else "정보없음"


class LLMOutputModel(BaseModel):
    """LLM 출력 데이터 전체 모델"""
    business_summary: str = Field(default="정보없음", description="사업 요약")
    major_products: List[str] = Field(default_factory=list, description="주요 제품/서비스 리스트")
    major_clients: str = Field(default="정보없음", description="주요 매출처/고객사")
    supply_chain: List[SupplyChainItem] = Field(default_factory=list, description="원재료-공급사 쌍 리스트")
    financial_value_chain: Optional[FinancialValueChain] = None
    capax_investment: str = Field(default="정보없음", description="설비투자 계획")
    cost_structure: str = Field(default="정보없음", description="비용 구조")
    keywords: List[str] = Field(default_factory=list, description="핵심 해시태그")
    revenue_by_segment: Dict[str, Union[float, int]] = Field(default_factory=dict, description="사업부문별 매출 비중 (%)")
    
    @field_validator('business_summary', 'major_clients', 'capax_investment', 'cost_structure', mode='before')
    @classmethod
    def clean_string_field(cls, v):
        if v is None:
            return "정보없음"
        result = str(v).strip()
        return result if result else "정보없음"
    
    @field_validator('major_products', 'keywords', mode='before')
    @classmethod
    def clean_string_list_field(cls, v):
        return clean_string_list(v)
    
    @field_validator('supply_chain', mode='before')
    @classmethod
    def clean_supply_chain(cls, v):
        if v is None:
            return []
        if isinstance(v, list):
            cleaned = []
            for item in v:
                if isinstance(item, dict):
                    cleaned.append(SupplyChainItem(**item))
                elif isinstance(item, str):
                    # 문자열이면 간단히 처리
                    cleaned.append(SupplyChainItem(item=item, supplier="정보없음"))
            return cleaned
        return []
    
    @field_validator('revenue_by_segment', mode='before')
    @classmethod
    def clean_revenue_by_segment(cls, v):
        if v is None:
            return {}
        if isinstance(v, dict):
            cleaned = {}
            for key, value in v.items():
                if key and key.strip():
                    parsed = parse_percentage(value)
                    if parsed is not None:
                        cleaned[str(key).strip()] = parsed
            return cleaned
        return {}
    
    @model_validator(mode='after')
    def validate_financial_company(self):
        """금융사인 경우 supply_chain은 빈 배열이어야 함"""
        if self.financial_value_chain is not None:
            if self.supply_chain:
                logger.warning("금융사인데 supply_chain이 있습니다. 빈 배열로 변경합니다.")
                self.supply_chain = []
        return self
    
    def to_dict(self) -> Dict[str, Any]:
        """딕셔너리로 변환 (supply_chain은 dict 형태로)"""
        data = self.model_dump()
        # supply_chain을 dict 리스트로 변환
        if 'supply_chain' in data:
            data['supply_chain'] = [
                {'item': item['item'], 'supplier': item['supplier']}
                for item in data['supply_chain']
            ]
        # financial_value_chain 처리
        if 'financial_value_chain' in data and data['financial_value_chain']:
            data['financial_value_chain'] = data['financial_value_chain']
        return data

