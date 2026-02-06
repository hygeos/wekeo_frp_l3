"""
Download and stores FRP products
"""

from wekeo_frp_l3.download import get_FRP_products

sday = "2022-07-15"
eday = "2022-07-23"

files = get_FRP_products(
    start_date = sday,
    end_date = eday,
)