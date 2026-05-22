"""原子层取数器模块"""
from .base_extractor import BaseExtractor
from .sale_extractor import SaleExtractor
from .purchase_extractor import PurchaseExtractor
from .scm_extractor import SCMExtractor
from .loss_extractor import LossExtractor
from .compose_extractor import ComposeExtractor
from .subsidy_extractor import SubsidyExtractor
from .promo_extractor import PromoExtractor
from .cost_price_extractor import CostPriceExtractor
from .price_extractor import PriceExtractor
from .inventory_extractor import InventoryExtractor

__all__ = [
    "BaseExtractor",
    "SaleExtractor",
    "PurchaseExtractor",
    "SCMExtractor",
    "LossExtractor",
    "ComposeExtractor",
    "SubsidyExtractor",
    "PromoExtractor",
    "CostPriceExtractor",
    "PriceExtractor",
    "InventoryExtractor",
]


def get_extractor(domain: str):
    """
    根据域名获取取数器类

    Args:
        domain: 域名称

    Returns:
        取数器类
    """
    extractors = {
        "sale": SaleExtractor,
        "purchase": PurchaseExtractor,
        "scm": SCMExtractor,
        "loss": LossExtractor,
        "compose": ComposeExtractor,
        "subsidy": SubsidyExtractor,
        "promo": PromoExtractor,
        "cost_price": CostPriceExtractor,
        "price": PriceExtractor,
        "inventory": InventoryExtractor,
    }

    if domain not in extractors:
        raise ValueError(f"Unknown domain: {domain}")

    return extractors[domain]


def extract_all_domains(settings, start_date: str, end_date: str) -> dict:
    """
    执行所有域的取数

    Args:
        settings: 配置对象
        start_date: 开始日期
        end_date: 结束日期

    Returns:
        各域取数结果路径字典
    """
    results = {}

    for domain in settings.atomic_domains:
        extractor_class = get_extractor(domain)
        with extractor_class(settings) as extractor:
            path = extractor.extract_and_save(start_date, end_date)
            results[domain] = path

    return results
