from app.models.article import Article, Summary
from app.models.user import UserInsight
from app.models.stock import Stock

# Axis 1: Ontology (The Brain)
from app.models.economic_variable import EconomicVariable

# Axis 2: Company Master (The Body)
from app.models.investor_sector import InvestorSector
from app.models.sector_ontology import SectorOntology
from app.models.sector_granular import SectorGranular
from app.models.econvar_granular_link import EconVarGranularLink
from app.models.company_alias import CompanyAlias

# Axis 3: Qualitative Data (The Context)
from app.models.company_detail import CompanyDetail
from app.models.company_detail_raw import CompanyDetailRaw
from app.models.company_detail_version import CompanyDetailVersion

# Axis 4: Quantitative Data (The Fuel)
from app.models.economic_history import EconomicHistory
from app.models.stock_price import StockPrice

# Axis 5: Logic & Links (The KG Layer)
from app.models.edge import Edge
from app.models.industry_edge import IndustryEdge

# 보완 테이블: Traceability
from app.models.processing_log import ProcessingLog
from app.models.broker_report import BrokerReport

__all__ = [
    "Article", 
    "Summary", 
    "UserInsight", 
    "Stock",
    # Axis 1
    "EconomicVariable",
    # Axis 2
    "InvestorSector",
    "SectorOntology",
    "SectorGranular",
    "EconVarGranularLink",
    "CompanyAlias",
    # Axis 3
    "CompanyDetail",
    "CompanyDetailRaw",
    "CompanyDetailVersion",
    # Axis 4
    "EconomicHistory",
    "StockPrice",
    # Axis 5
    "Edge",
    "IndustryEdge",
    # 보완
    "ProcessingLog",
    "BrokerReport",
]

