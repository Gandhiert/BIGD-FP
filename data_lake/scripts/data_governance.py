#!/usr/bin/env python3
"""
Data Governance & Metadata Management untuk Data Lakehouse
Mengelola metadata, lineage, access control, dan compliance
"""

import os
import json
import hashlib
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import pandas as pd
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataCatalog:
    """
    Data catalog untuk metadata management dan discovery
    """
    
    def __init__(self, catalog_db_path: str = "../logs/data_catalog.db"):
        self.db_path = catalog_db_path
        self._init_database()
    
    def _init_database(self):
        """Initialize database schema untuk catalog"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Table untuk dataset metadata
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS datasets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            path TEXT NOT NULL,
            format TEXT NOT NULL,
            size_bytes INTEGER,
            row_count INTEGER,
            column_count INTEGER,
            created_date TEXT NOT NULL,
            last_updated TEXT NOT NULL,
            checksum TEXT,
            description TEXT,
            tags TEXT,
            owner TEXT,
            classification TEXT
        )
        ''')
        
        # Table untuk column metadata
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS columns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dataset_id INTEGER,
            column_name TEXT NOT NULL,
            data_type TEXT NOT NULL,
            nullable BOOLEAN,
            unique_values INTEGER,
            null_count INTEGER,
            min_value TEXT,
            max_value TEXT,
            description TEXT,
            pii_flag BOOLEAN DEFAULT FALSE,
            FOREIGN KEY (dataset_id) REFERENCES datasets (id)
        )
        ''')
        
        # Table untuk data lineage
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS lineage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_dataset TEXT NOT NULL,
            target_dataset TEXT NOT NULL,
            transformation_type TEXT NOT NULL,
            transformation_details TEXT,
            created_date TEXT NOT NULL,
            created_by TEXT
        )
        ''')
        
        # Table untuk access logs
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS access_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dataset_name TEXT NOT NULL,
            user_id TEXT NOT NULL,
            access_type TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            ip_address TEXT,
            success BOOLEAN
        )
        ''')
        
        conn.commit()
        conn.close()
        
        logger.info(f"✅ Data catalog initialized at {self.db_path}")
    
    def register_dataset(self, 
                        name: str, 
                        path: str, 
                        format: str,
                        description: str = "",
                        owner: str = "system",
                        classification: str = "internal",
                        tags: List[str] = None) -> int:
        """
        Register dataset baru dalam catalog
        
        Returns:
            Dataset ID
        """
        try:
            # Calculate metadata
            if os.path.exists(path):
                size_bytes = os.path.getsize(path)
                checksum = self._calculate_checksum(path)
                
                # Untuk parquet files, dapatkan row/column count
                if format.lower() == 'parquet':
                    try:
                        df = pd.read_parquet(path)
                        row_count = len(df)
                        column_count = len(df.columns)
                    except:
                        row_count = column_count = 0
                else:
                    row_count = column_count = 0
            else:
                size_bytes = row_count = column_count = 0
                checksum = ""
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Insert dataset
            cursor.execute('''
            INSERT OR REPLACE INTO datasets 
            (name, path, format, size_bytes, row_count, column_count, 
             created_date, last_updated, checksum, description, tags, owner, classification)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                name, path, format, size_bytes, row_count, column_count,
                datetime.now().isoformat(), datetime.now().isoformat(),
                checksum, description, json.dumps(tags or []), owner, classification
            ))
            
            dataset_id = cursor.lastrowid
            
            # Register column metadata untuk parquet files
            if format.lower() == 'parquet' and os.path.exists(path):
                self._register_columns(cursor, dataset_id, path)
            
            conn.commit()
            conn.close()
            
            logger.info(f"📝 Registered dataset: {name} (ID: {dataset_id})")
            return dataset_id
            
        except Exception as e:
            logger.error(f"❌ Failed to register dataset {name}: {e}")
            return -1
    
    def _register_columns(self, cursor, dataset_id: int, file_path: str):
        """Register column metadata untuk parquet file"""
        try:
            df = pd.read_parquet(file_path)
            
            for column in df.columns:
                col_data = df[column]
                
                # Detect PII (simple heuristic)
                pii_flag = any(keyword in column.lower() for keyword in [
                    'email', 'phone', 'ssn', 'address', 'name', 'id'
                ])
                
                cursor.execute('''
                INSERT OR REPLACE INTO columns
                (dataset_id, column_name, data_type, nullable, unique_values, 
                 null_count, min_value, max_value, pii_flag)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    dataset_id, column, str(col_data.dtype), col_data.isna().any(),
                    col_data.nunique(), col_data.isna().sum(),
                    str(col_data.min()) if pd.api.types.is_numeric_dtype(col_data) else None,
                    str(col_data.max()) if pd.api.types.is_numeric_dtype(col_data) else None,
                    pii_flag
                ))
                
        except Exception as e:
            logger.warning(f"Could not register columns for {file_path}: {e}")
    
    def _calculate_checksum(self, file_path: str) -> str:
        """Calculate MD5 checksum untuk file integrity"""
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except:
            return ""
    
    def add_lineage(self, 
                   source: str, 
                   target: str, 
                   transformation_type: str,
                   details: str = "",
                   created_by: str = "system"):
        """Add data lineage relationship"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
        INSERT INTO lineage (source_dataset, target_dataset, transformation_type, 
                           transformation_details, created_date, created_by)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (source, target, transformation_type, details, 
              datetime.now().isoformat(), created_by))
        
        conn.commit()
        conn.close()
        
        logger.info(f"🔗 Added lineage: {source} -> {target} ({transformation_type})")
    
    def log_access(self, 
                  dataset_name: str, 
                  user_id: str, 
                  access_type: str,
                  ip_address: str = "",
                  success: bool = True):
        """Log data access untuk audit trail"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
        INSERT INTO access_logs (dataset_name, user_id, access_type, 
                               timestamp, ip_address, success)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (dataset_name, user_id, access_type, 
              datetime.now().isoformat(), ip_address, success))
        
        conn.commit()
        conn.close()
    
    def search_datasets(self, query: str = "", tags: List[str] = None) -> List[Dict]:
        """Search datasets dalam catalog"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        sql = "SELECT * FROM datasets WHERE 1=1"
        params = []
        
        if query:
            sql += " AND (name LIKE ? OR description LIKE ?)"
            params.extend([f"%{query}%", f"%{query}%"])
        
        if tags:
            for tag in tags:
                sql += " AND tags LIKE ?"
                params.append(f"%{tag}%")
        
        cursor.execute(sql, params)
        results = cursor.fetchall()
        
        columns = [desc[0] for desc in cursor.description]
        datasets = [dict(zip(columns, row)) for row in results]
        
        conn.close()
        return datasets
    
    def get_lineage_graph(self, dataset_name: str) -> Dict:
        """Get lineage graph untuk dataset"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get upstream dependencies
        cursor.execute('''
        SELECT source_dataset, transformation_type, transformation_details
        FROM lineage WHERE target_dataset = ?
        ''', (dataset_name,))
        upstream = cursor.fetchall()
        
        # Get downstream dependencies
        cursor.execute('''
        SELECT target_dataset, transformation_type, transformation_details
        FROM lineage WHERE source_dataset = ?
        ''', (dataset_name,))
        downstream = cursor.fetchall()
        
        conn.close()
        
        return {
            'dataset': dataset_name,
            'upstream': [{'source': row[0], 'type': row[1], 'details': row[2]} for row in upstream],
            'downstream': [{'target': row[0], 'type': row[1], 'details': row[2]} for row in downstream]
        }
    
    def get_data_quality_report(self) -> Dict:
        """Generate data quality report"""
        conn = sqlite3.connect(self.db_path)
        
        # Dataset metrics
        datasets_df = pd.read_sql("SELECT * FROM datasets", conn)
        columns_df = pd.read_sql("SELECT * FROM columns", conn)
        
        conn.close()
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'summary': {
                'total_datasets': len(datasets_df),
                'total_columns': len(columns_df),
                'datasets_with_pii': len(columns_df[columns_df['pii_flag'] == True]['dataset_id'].unique()),
                'avg_dataset_size_mb': (datasets_df['size_bytes'].mean() / 1024 / 1024) if len(datasets_df) > 0 else 0
            },
            'quality_issues': []
        }
        
        # Check for quality issues
        if len(datasets_df) > 0:
            # Empty datasets
            empty_datasets = datasets_df[datasets_df['row_count'] == 0]
            if len(empty_datasets) > 0:
                report['quality_issues'].append({
                    'type': 'empty_datasets',
                    'count': len(empty_datasets),
                    'datasets': empty_datasets['name'].tolist()
                })
            
            # Large datasets tanpa description
            no_desc = datasets_df[datasets_df['description'] == '']
            if len(no_desc) > 0:
                report['quality_issues'].append({
                    'type': 'missing_descriptions',
                    'count': len(no_desc),
                    'datasets': no_desc['name'].tolist()
                })
        
        return report

class DataSecurity:
    """
    Data security dan access control management
    """
    
    def __init__(self, security_config_path: str = "../config/security.json"):
        self.config_path = security_config_path
        self.config = self._load_security_config()
    
    def _load_security_config(self) -> Dict:
        """Load security configuration"""
        default_config = {
            'encryption': {
                'enabled': True,
                'algorithm': 'AES-256',
                'key_rotation_days': 90
            },
            'access_control': {
                'default_policy': 'deny',
                'roles': {
                    'admin': ['read', 'write', 'delete'],
                    'analyst': ['read'],
                    'viewer': ['read']
                }
            },
            'audit': {
                'log_all_access': True,
                'retention_days': 365
            },
            'compliance': {
                'gdpr_enabled': True,
                'data_retention_days': 2555  # 7 years
            }
        }
        
        try:
            os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r') as f:
                    return json.load(f)
            else:
                # Create default config
                with open(self.config_path, 'w') as f:
                    json.dump(default_config, f, indent=2)
                return default_config
        except Exception as e:
            logger.warning(f"Could not load security config: {e}")
            return default_config
    
    def check_access_permission(self, 
                              user_id: str, 
                              dataset_name: str, 
                              operation: str) -> bool:
        """
        Check apakah user memiliki permission untuk operasi tertentu
        
        Args:
            user_id: User identifier
            dataset_name: Dataset name
            operation: 'read', 'write', 'delete'
        
        Returns:
            True jika allowed, False jika denied
        """
        # Simplified access control - bisa diperluas dengan RBAC/ABAC
        
        # Admin always allowed
        if user_id.startswith('admin'):
            return True
        
        # Check dataset classification
        if 'sensitive' in dataset_name.lower() and operation in ['write', 'delete']:
            return False
        
        # Check user role (simplified)
        if user_id.startswith('analyst'):
            return operation == 'read'
        
        # Default based on configuration
        return self.config['access_control']['default_policy'] == 'allow'
    
    def mask_pii_data(self, df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """
        Mask PII data untuk privacy protection
        
        Args:
            df: DataFrame dengan data
            columns: List kolom yang mengandung PII
        
        Returns:
            DataFrame dengan PII data yang di-mask
        """
        masked_df = df.copy()
        
        for col in columns:
            if col in masked_df.columns:
                # Email masking
                if 'email' in col.lower():
                    masked_df[col] = masked_df[col].astype(str).str.replace(
                        r'(.{2}).*@(.*)\.(.{2,})', 
                        r'\1***@\2.\3', 
                        regex=True
                    )
                
                # General PII masking
                else:
                    masked_df[col] = masked_df[col].astype(str).str.replace(
                        r'(.{2}).*(.{2})', 
                        r'\1***\2', 
                        regex=True
                    )
        
        return masked_df
    
    def generate_compliance_report(self) -> Dict:
        """Generate compliance report (GDPR, etc.)"""
        report = {
            'timestamp': datetime.now().isoformat(),
            'compliance_framework': 'GDPR',
            'status': 'COMPLIANT',
            'findings': [],
            'recommendations': []
        }
        
        # Check data retention
        if self.config['compliance']['data_retention_days'] > 2555:
            report['findings'].append("Data retention period exceeds recommended 7 years")
        
        # Check encryption
        if not self.config['encryption']['enabled']:
            report['status'] = 'NON_COMPLIANT'
            report['findings'].append("Encryption not enabled")
            report['recommendations'].append("Enable data encryption")
        
        return report

class DataGovernanceManager:
    """
    Central manager untuk semua aspek data governance
    """
    
    def __init__(self):
        self.catalog = DataCatalog()
        self.security = DataSecurity()
        
        logger.info("🏛️ Data Governance Manager initialized")
    
    def onboard_new_dataset(self, 
                           name: str, 
                           path: str, 
                           format: str,
                           source_datasets: List[str] = None,
                           **metadata) -> int:
        """
        Complete onboarding process untuk dataset baru
        """
        logger.info(f"📋 Onboarding new dataset: {name}")
        
        # Register dalam catalog
        dataset_id = self.catalog.register_dataset(
            name=name,
            path=path,
            format=format,
            description=metadata.get('description', ''),
            owner=metadata.get('owner', 'system'),
            classification=metadata.get('classification', 'internal'),
            tags=metadata.get('tags', [])
        )
        
        # Add lineage jika ada source datasets
        if source_datasets:
            for source in source_datasets:
                self.catalog.add_lineage(
                    source=source,
                    target=name,
                    transformation_type='ETL',
                    details=f"Transformed from {source} via ETL pipeline"
                )
        
        # Log access untuk audit
        self.catalog.log_access(
            dataset_name=name,
            user_id='system',
            access_type='create',
            success=True
        )
        
        logger.info(f"✅ Dataset {name} successfully onboarded (ID: {dataset_id})")
        return dataset_id
    
    def discover_and_catalog_warehouse(self, warehouse_path: str = "../warehouse"):
        """
        Automatically discover dan catalog semua files di warehouse
        """
        logger.info(f"🔍 Auto-discovering datasets in {warehouse_path}")
        
        if not os.path.exists(warehouse_path):
            logger.warning(f"Warehouse path does not exist: {warehouse_path}")
            return
        
        discovered_count = 0
        
        for file_path in Path(warehouse_path).glob('**/*'):
            if file_path.is_file() and file_path.suffix in ['.parquet', '.csv', '.json']:
                name = file_path.stem
                format = file_path.suffix[1:]  # Remove dot
                
                # Auto-detect description berdasarkan nama file
                descriptions = {
                    'reviews': 'Game reviews dari Steam platform dengan sentiment analysis',
                    'games': 'Game catalog dengan metadata lengkap',
                    'logs': 'Server logs untuk monitoring dan debugging',
                    'configs': 'Game server configuration data',
                    'catalog': 'Game catalog dalam format XML yang sudah di-transform'
                }
                
                description = descriptions.get(name, f"Auto-discovered {format} dataset")
                
                self.onboard_new_dataset(
                    name=name,
                    path=str(file_path),
                    format=format,
                    description=description,
                    owner='auto-discovery',
                    classification='internal',
                    tags=['auto-discovered', 'warehouse']
                )
                
                discovered_count += 1
        
        logger.info(f"✅ Auto-discovery completed. Found {discovered_count} datasets")
    
    def run_governance_audit(self) -> Dict:
        """
        Run comprehensive governance audit
        """
        logger.info("🔍 Running comprehensive governance audit...")
        
        audit_report = {
            'timestamp': datetime.now().isoformat(),
            'data_quality': self.catalog.get_data_quality_report(),
            'compliance': self.security.generate_compliance_report(),
            'recommendations': []
        }
        
        # Add recommendations berdasarkan findings
        if audit_report['data_quality']['quality_issues']:
            audit_report['recommendations'].append(
                "Address data quality issues identified in the report"
            )
        
        if audit_report['compliance']['status'] != 'COMPLIANT':
            audit_report['recommendations'].append(
                "Resolve compliance issues to maintain regulatory compliance"
            )
        
        # Save audit report
        audit_file = f"../logs/governance_audit_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        os.makedirs(os.path.dirname(audit_file), exist_ok=True)
        
        with open(audit_file, 'w') as f:
            json.dump(audit_report, f, indent=2)
        
        logger.info(f"📊 Governance audit completed. Report saved to {audit_file}")
        
        return audit_report

def main():
    """Command line interface untuk data governance"""
    import sys
    
    governance = DataGovernanceManager()
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == 'discover':
            governance.discover_and_catalog_warehouse()
        
        elif command == 'audit':
            result = governance.run_governance_audit()
            print(json.dumps(result, indent=2))
        
        elif command == 'search':
            query = sys.argv[2] if len(sys.argv) > 2 else ""
            results = governance.catalog.search_datasets(query)
            print(json.dumps(results, indent=2))
        
        elif command == 'lineage':
            if len(sys.argv) > 2:
                dataset = sys.argv[2]
                lineage = governance.catalog.get_lineage_graph(dataset)
                print(json.dumps(lineage, indent=2))
            else:
                print("Usage: python data_governance.py lineage <dataset_name>")
        
        else:
            print("Available commands: discover, audit, search, lineage")
    
    else:
        print("Data Governance Manager")
        print("Commands:")
        print("  discover  - Auto-discover and catalog warehouse datasets")
        print("  audit     - Run governance audit")
        print("  search    - Search datasets in catalog")
        print("  lineage   - Show dataset lineage")

if __name__ == "__main__":
    main()