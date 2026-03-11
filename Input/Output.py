# Databricks notebook source
# COMMAND ----------

# Migrated from Splunk to Databricks logging
%run './databricks_logger'

import atexit
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import traceback
import sys
import os

# COMMAND ----------

# Initialize Databricks logger
logger = get_databricks_logger(__name__)

# COMMAND ----------

# Commented out Splunk-related imports and initialization
# %run './splunk_logger'
# import requests
# splunk_secret = get_secret("splunk-credentials")

# COMMAND ----------

def flush_logger_on_exit():
    """Flush logger on application exit"""
    try:
        logger.flush()
    except Exception as e:
        print(f"Error flushing logger on exit: {e}")

# Register exit handler for logger cleanup
atexit.register(flush_logger_on_exit)

# COMMAND ----------

class AnalyticsCommons:
    """
    Common utilities for analytics operations
    Migrated from Splunk to Databricks logging
    """
    
    def __init__(self):
        self.logger = logger
        
    def process_data(self, data_frame, config_params=None):
        """
        Process analytics data with logging
        
        Args:
            data_frame: Input DataFrame to process
            config_params: Configuration parameters
            
        Returns:
            Processed DataFrame
        """
        try:
            self.logger.info("Starting data processing")
            self.logger.info(f"Input data shape: {data_frame.shape}")
            
            # Business logic remains unchanged
            if config_params is None:
                config_params = {}
                
            processed_df = data_frame.copy()
            
            # Apply transformations based on config
            if 'filter_column' in config_params:
                filter_col = config_params['filter_column']
                filter_val = config_params.get('filter_value', None)
                if filter_val is not None:
                    processed_df = processed_df[processed_df[filter_col] == filter_val]
                    
            # Add timestamp if required
            if config_params.get('add_timestamp', False):
                processed_df['processed_timestamp'] = datetime.now()
                
            self.logger.info(f"Processed data shape: {processed_df.shape}")
            self.logger.info("Data processing completed successfully")
            
            # Flush logger after processing
            logger.flush()
            
            return processed_df
            
        except Exception as e:
            self.logger.error(f"Error in data processing: {str(e)}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            logger.flush()
            raise
            
    def validate_data_quality(self, data_frame, validation_rules=None):
        """
        Validate data quality with comprehensive logging
        
        Args:
            data_frame: DataFrame to validate
            validation_rules: Dictionary of validation rules
            
        Returns:
            Dictionary with validation results
        """
        try:
            self.logger.info("Starting data quality validation")
            
            validation_results = {
                'total_rows': len(data_frame),
                'total_columns': len(data_frame.columns),
                'null_counts': {},
                'duplicate_rows': 0,
                'validation_passed': True,
                'issues': []
            }
            
            # Check for null values
            null_counts = data_frame.isnull().sum()
            validation_results['null_counts'] = null_counts.to_dict()
            
            # Check for duplicates
            duplicate_count = data_frame.duplicated().sum()
            validation_results['duplicate_rows'] = duplicate_count
            
            # Apply custom validation rules
            if validation_rules:
                for rule_name, rule_config in validation_rules.items():
                    try:
                        if rule_config['type'] == 'range_check':
                            column = rule_config['column']
                            min_val = rule_config.get('min_value')
                            max_val = rule_config.get('max_value')
                            
                            if min_val is not None:
                                violations = (data_frame[column] < min_val).sum()
                                if violations > 0:
                                    validation_results['issues'].append(f"{rule_name}: {violations} values below minimum")
                                    
                            if max_val is not None:
                                violations = (data_frame[column] > max_val).sum()
                                if violations > 0:
                                    validation_results['issues'].append(f"{rule_name}: {violations} values above maximum")
                                    
                    except Exception as rule_error:
                        self.logger.warning(f"Error applying validation rule {rule_name}: {str(rule_error)}")
                        
            # Determine overall validation status
            if validation_results['issues'] or duplicate_count > 0:
                validation_results['validation_passed'] = False
                
            self.logger.info(f"Data quality validation completed: {validation_results['validation_passed']}")
            self.logger.info(f"Total issues found: {len(validation_results['issues'])}")
            
            # Flush logger after validation
            logger.flush()
            
            return validation_results
            
        except Exception as e:
            self.logger.error(f"Error in data quality validation: {str(e)}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            logger.flush()
            raise
            
    def generate_analytics_report(self, data_frame, report_config=None):
        """
        Generate analytics report with detailed logging
        
        Args:
            data_frame: Source DataFrame
            report_config: Report configuration parameters
            
        Returns:
            Dictionary containing report data
        """
        try:
            self.logger.info("Starting analytics report generation")
            
            if report_config is None:
                report_config = {}
                
            report = {
                'generated_at': datetime.now().isoformat(),
                'data_summary': {},
                'metrics': {},
                'insights': []
            }
            
            # Generate basic data summary
            report['data_summary'] = {
                'total_records': len(data_frame),
                'columns': list(data_frame.columns),
                'data_types': data_frame.dtypes.to_dict(),
                'memory_usage': data_frame.memory_usage(deep=True).sum()
            }
            
            # Calculate metrics for numeric columns
            numeric_columns = data_frame.select_dtypes(include=[np.number]).columns
            for col in numeric_columns:
                report['metrics'][col] = {
                    'mean': float(data_frame[col].mean()),
                    'median': float(data_frame[col].median()),
                    'std': float(data_frame[col].std()),
                    'min': float(data_frame[col].min()),
                    'max': float(data_frame[col].max())
                }
                
            # Generate insights based on configuration
            if report_config.get('include_insights', True):
                # Add correlation insights for numeric data
                if len(numeric_columns) > 1:
                    correlation_matrix = data_frame[numeric_columns].corr()
                    high_correlations = []
                    
                    for i in range(len(correlation_matrix.columns)):
                        for j in range(i+1, len(correlation_matrix.columns)):
                            corr_val = correlation_matrix.iloc[i, j]
                            if abs(corr_val) > 0.7:  # High correlation threshold
                                high_correlations.append({
                                    'column1': correlation_matrix.columns[i],
                                    'column2': correlation_matrix.columns[j],
                                    'correlation': float(corr_val)
                                })
                                
                    if high_correlations:
                        report['insights'].append({
                            'type': 'high_correlation',
                            'data': high_correlations
                        })
                        
            self.logger.info("Analytics report generation completed successfully")
            self.logger.info(f"Report contains {len(report['metrics'])} metrics and {len(report['insights'])} insights")
            
            # Flush logger after report generation
            logger.flush()
            
            return report
            
        except Exception as e:
            self.logger.error(f"Error in analytics report generation: {str(e)}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            logger.flush()
            raise

# COMMAND ----------

def initialize_analytics_commons():
    """
    Initialize analytics commons with proper logging setup
    """
    try:
        logger.info("Initializing Analytics Commons")
        commons = AnalyticsCommons()
        logger.info("Analytics Commons initialized successfully")
        logger.flush()
        return commons
    except Exception as e:
        logger.error(f"Error initializing Analytics Commons: {str(e)}")
        logger.flush()
        raise

# COMMAND ----------

def cleanup_resources():
    """
    Cleanup function to ensure proper resource management
    """
    try:
        logger.info("Cleaning up analytics resources")
        # Perform any necessary cleanup
        logger.info("Resource cleanup completed")
        logger.flush()
    except Exception as e:
        logger.error(f"Error during resource cleanup: {str(e)}")
        logger.flush()

# Register cleanup function
atexit.register(cleanup_resources)

# COMMAND ----------

# Main execution block
if __name__ == "__main__":
    try:
        logger.info("Starting Analytics Commons utility execution")
        
        # Initialize commons
        analytics_commons = initialize_analytics_commons()
        
        logger.info("Analytics Commons utility ready for use")
        logger.flush()
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        logger.flush()
        sys.exit(1)

# COMMAND ----------

# Final flush to ensure all logs are written
logger.flush()
