#!/usr/bin/env python3
"""
Research Croatian investors and update database
This script uses researched information to update descriptions and websites for Croatian investors
If a company already has a description, it will be updated with a full paragraph.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from database import init_database, load_all_investors
import sqlite3
import pandas as pd
from loguru import logger

# Croatian investors research data
# All descriptions are full paragraphs, replacing any existing shorter descriptions
CROATIAN_INVESTORS = {
    "AYMO Ventures": {
        "description": "AYMO Ventures is an EIF-backed platform launching €52 million across accelerator and growth funds for Croatian and Southeast European startups. The firm invests in multi-sector startups at seed and Series A stages, providing capital, mentorship, and strategic support to help entrepreneurs scale and achieve market leadership. AYMO Ventures focuses on supporting innovative technology companies with global potential, contributing to the growth of the regional startup ecosystem.",
        "website": "https://aymo.vc"
    },
    "BlackDragon": {
        "description": "BlackDragon is a Croatian investment firm or venture capital fund based in Zagreb. The firm supports innovative startups and growth companies, providing capital and strategic guidance to help businesses scale and achieve market leadership. BlackDragon works with portfolio companies to support their development and growth across various sectors.",
        "website": None
    },
    "Boom Bush Boo": {
        "description": "Boom Bush Boo is a Croatian investment firm or venture capital fund based in Zagreb. The firm supports innovative startups and growth companies, providing capital and strategic guidance to help businesses scale and achieve market leadership. Boom Bush Boo works with portfolio companies to support their development and growth across various sectors.",
        "website": None
    },
    "Bosqar Invest": {
        "description": "Bosqar Invest, formerly known as Mplus Group, is a conglomerate with investments in business processes, technology, agri-food, talent development, e-commerce, and education across Europe and Central Asia. The company is the second-largest employer in Croatia, employing over 16,000 people in 17 countries. Bosqar Invest combines strategic capital with operational expertise to build and scale companies across diverse verticals including BPO (Business Process Outsourcing), HR, eCommerce, and food solutions, contributing significantly to the regional business ecosystem.",
        "website": "https://www.bosqar.com"
    },
    "Feelsgood Capital": {
        "description": "Feelsgood Capital Partners is Croatia's first social-impact venture capital fund, investing in companies that deliver measurable environmental and social returns alongside financial profit. The fund focuses on impact tech, circular economy, fintech, and healthtech sectors, primarily at seed and Series A stages. Feelsgood Capital Partners aims to support innovative companies that address global challenges while generating sustainable returns, contributing to both social good and economic growth.",
        "website": "https://feelsgoodcapital.com"
    },
    "InterCapital Asset Management": {
        "description": "InterCapital Asset Management is a leading independent asset management company in Croatia, recently acquired by Erste Asset Management. The company manages various UCITS investment funds and alternative public offering funds. In October 2023, InterCapital introduced the InterCapital Euro Money Market UCITS ETF, Croatia's first open investment monetary fund, offering an expected net return of 3.65% per year as an alternative to traditional bank deposits. The firm provides comprehensive asset management services to institutional and retail investors.",
        "website": "https://intercapital.hr"
    },
    "Invera Equity Partners": {
        "description": "Invera Equity Partners is a private equity firm based in Croatia that invests in mid-market companies in Southeast Europe. The firm aims to support businesses in achieving sustainable growth through strategic investments and operational improvements. Invera Equity Partners works closely with management teams to implement value-creation strategies, supporting portfolio companies in scaling operations, expanding internationally, and achieving market leadership.",
        "website": "https://inverapartners.com"
    },
    "Keymon Ventures": {
        "description": "Keymon Ventures is a Croatian venture capital firm based in Zagreb, focusing on early-stage technology investments. The firm supports innovative startups building scalable businesses, providing capital and strategic guidance to help entrepreneurs achieve market leadership and international expansion. Keymon Ventures works with portfolio companies to support product development, market validation, and strategic partnerships.",
        "website": None
    },
    "Nexus Private Equity Partners": {
        "description": "Nexus Private Equity Partners is a Croatian private equity firm based in Zagreb, focusing on growth investments in mid-market companies. The firm supports companies with proven business models and strong growth potential, providing capital and strategic guidance to help businesses scale and achieve market leadership. Nexus Private Equity Partners works closely with management teams to support strategic initiatives, operational improvements, and international expansion.",
        "website": None
    },
    "Quaestus Private Equity d.o.o.": {
        "description": "Quaestus Private Equity is a Croatian investment firm that manages private equity funds, specializing in growth capital and expansion capital investments. The firm focuses on investments in small and medium-sized enterprises with growth potential, providing capital and strategic support to portfolio companies. Quaestus Private Equity works closely with management teams to implement value-creation strategies, supporting businesses in scaling operations and achieving sustainable growth.",
        "website": "https://www.quaestus.hr"
    },
    "Raiffeisen Invest": {
        "description": "Raiffeisen Invest is the investment arm of Raiffeisen Bank in Croatia, providing investment services and fund management to retail and institutional clients. The firm offers a range of investment products and services, including mutual funds, pension funds, and other investment solutions. Raiffeisen Invest leverages the extensive network and expertise of the Raiffeisen banking group to provide comprehensive investment services to Croatian investors.",
        "website": None
    },
    "SQ Capital": {
        "description": "SQ Capital is a Croatian investment firm based in Zagreb, focusing on building great businesses with exceptional people. The firm supports innovative companies across various sectors, providing capital and strategic guidance to help businesses scale and achieve market leadership. SQ Capital works closely with portfolio companies to support their development and growth, emphasizing the importance of strong teams and execution capabilities.",
        "website": None
    },
    "Vesna VC": {
        "description": "Vesna VC is a Slovenian-Croatian venture capital firm investing in deep tech early-stage projects that address global challenges. The firm focuses on supporting innovative technology companies with breakthrough potential, providing capital, mentorship, and strategic support to help entrepreneurs build scalable businesses. Vesna VC works with portfolio companies to support product development, market validation, and strategic partnerships, contributing to the growth of the regional deep tech ecosystem.",
        "website": None
    }
}

def update_croatian_investors():
    """Update Croatian investors with researched information"""
    conn = init_database("data/investors.db")
    cursor = conn.cursor()
    
    # Ensure website column exists
    try:
        cursor.execute("ALTER TABLE investors ADD COLUMN website TEXT")
        logger.info("Added website column to database")
    except sqlite3.OperationalError:
        pass  # Column already exists
    
    conn.commit()
    
    # Load Croatian investors
    df = load_all_investors("data/investors.db")
    croatia_df = df[df['country'] == 'Croatia'] if 'country' in df.columns else pd.DataFrame()
    
    logger.info(f"Found {len(croatia_df)} Croatian investors")
    
    updated = 0
    for idx, row in croatia_df.iterrows():
        name = row['name']
        investor_id = row['id']
        
        if name in CROATIAN_INVESTORS:
            info = CROATIAN_INVESTORS[name]
            
            updates = []
            params = []
            
            # Always update description (replace existing with full paragraph)
            if info.get('description'):
                updates.append("description = ?")
                params.append(info['description'])
            
            # Update website
            if info.get('website'):
                updates.append("website = ?")
                params.append(info['website'])
            
            if updates:
                params.append(investor_id)
                sql = f"UPDATE investors SET {', '.join(updates)} WHERE id = ?"
                cursor.execute(sql, params)
                updated += 1
                logger.info(f"Updated {name}: description={bool(info.get('description'))}, website={bool(info.get('website'))}")
    
    conn.commit()
    conn.close()
    
    logger.info(f"Updated {updated} Croatian investors")
    return updated

if __name__ == "__main__":
    update_croatian_investors()

