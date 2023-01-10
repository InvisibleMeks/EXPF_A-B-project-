import yaml
import config
import abc
import pandas as pd
from pandas import DataFrame
import numpy as np
from yaml.loader import SafeLoader
from os import listdir
import operator


def _load_yaml_preset(preset="default"):
    preset_path = config.PATH_METRIC_CONFIGS + preset
    metrics_to_load = listdir(preset_path)
    metrics = []
    for metric in metrics_to_load:
        with open(preset_path + "/" + metric) as f:
            metrics.append(yaml.load(f, Loader=SafeLoader))
    return metrics

class Metric:
   
    def __init__(self, metric_config):
        self.name = metric_config.get("name", config.DEFAULT_VALUE)
        self.type = metric_config.get("type", config.DEFAULT_METRIC_TYPE)
        self.level = metric_config.get("level", config.DEFAULT_UNIT_LEVEL)
        self.estimator = metric_config.get("estimator", config.DEFAULT_ESTIMATOR)
        
        self.numerator = metric_config.get("numerator", config.DEFAULT_VALUE)
        self.numerator_aggregation_field = self.numerator.get("aggregation_field", config.DEFAULT_VALUE)
        
        self.denominator = metric_config.get("denominator", config.DEFAULT_VALUE)
        self.denominator_aggregation_field = self.denominator.get("aggregation_field", config.DEFAULT_VALUE)
        
        numerator_aggregation_function = self.numerator.get("aggregation_function", config.DEFAULT_VALUE)
        denominator_aggregation_function = self.denominator.get("aggregation_function", config.DEFAULT_VALUE)
        self.numerator_aggregation_function = self._map_aggregation_function(numerator_aggregation_function)
        self.denominator_aggregation_function = self._map_aggregation_function(denominator_aggregation_function)
        
        numerator_conditions_dict = metric_config.get("numerator_conditions", config.DEFAULT_CONDITION)
        self.numerator_conditions = self._check_condition(numerator_conditions_dict)
        denominator_conditions_dict = metric_config.get("denominator_conditions", config.DEFAULT_CONDITION)
        self.denominator_conditions = self._check_condition(denominator_conditions_dict)
        
        
        
    @staticmethod
    def _map_aggregation_function(aggregation_function):
        mappings = {
            "count_distinct": pd.Series.nunique,
            "sum": np.sum
        }
        if aggregation_function == config.DEFAULT_VALUE:
            raise ValueError("No aggregation_function found")

        agg_func = mappings[aggregation_function]
        if aggregation_function not in mappings.keys():
            raise ValueError(f"{aggregation_function} not found in mappings")
        return agg_func

    @staticmethod
    def _check_condition(conditions_dict: dict):
        #словарь для расшифровки фильтров
        signs_dict = {
            'equal': operator.eq,
            'not_equal': operator.ne 
        }
      
        if (conditions_dict == config.DEFAULT_CONDITION):
            return(config.DEFAULT_CONDITION)
        else:
            if (len(conditions_dict) == 1):
                if (conditions_dict[0].keys() == config.DEFAULT_CONDITION.keys()): 
                    condition_field = str(conditions_dict[0]['condition_field'])
                    comparison_value = str(conditions_dict[0]['comparison_value'])
                    try:
                        comparison_sign = signs_dict[conditions_dict[0]['comparison_sign']]
                        #numerator_conditions = condition_field + " " + comparison_sign + " " + comparison_value
                        return(
                           {
                                'condition_field': condition_field,
                                'comparison_sign': comparison_sign,
                                'comparison_value': comparison_value
                           }    
                        )
                    except KeyError as e:
                        return(config.DEFAULT_CONDITION)                             
            else:
                return(config.DEFAULT_CONDITION)
        
    
class CalculateMetric:
    def __init__(self, metric: Metric):
        self.metric = metric

    def __call__(self, df):
        
        
        #without conditions
#         return df.groupby([config.VARIANT_COL, self.metric.level]).apply(
#                lambda df: pd.Series({
#                    "num": self.metric.numerator_aggregation_function(df[self.metric.numerator_aggregation_field]),
#                    "den": self.metric.denominator_aggregation_function(df[self.metric.denominator_aggregation_field]),
#                    "n": pd.Series.nunique(df[self.metric.level])
#                })
#            ).reset_index()
        
        #with conditions
        return df.groupby([config.VARIANT_COL, self.metric.level]).apply(
                lambda df: pd.Series({
                    "num": self.metric.numerator_aggregation_function(df.loc[self._df_cond(df,self.metric.numerator_conditions)]
                                                                                     [self.metric.numerator_aggregation_field]),
                    "den": self.metric.denominator_aggregation_function(df.loc[self._df_cond(df,self.metric.denominator_conditions)]
                                                                                      [self.metric.denominator_aggregation_field]),
                    "n": pd.Series.nunique(df[self.metric.level])
                })
            ).reset_index()
    
    @staticmethod
    def _df_cond(df:DataFrame, cond_dict: dict):
        
        if (cond_dict == config.DEFAULT_CONDITION):
            return()
        else:
            if (cond_dict.keys() == config.DEFAULT_CONDITION.keys()): 
                return(cond_dict["comparison_sign"](df[cond_dict["condition_field"]],cond_dict["comparison_value"]))                       
            else:
                return()
           