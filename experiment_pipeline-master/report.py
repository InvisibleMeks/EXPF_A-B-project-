import pandas as pd
import numpy as np
import abc
import utils
import config as cfg
from itertools import product
from metric_builder import Metric, CalculateMetric
from stattests import TTestFromStats, Mann_Whitney_U_test, Z_test, calculate_statistics, calculate_linearization, data_by_groups


class Report:
    def __init__(self, report):
        self.report = report


class BuildMetricReport:
    def __call__(self, calculated_metric, metric_items) -> Report:
        ttest = TTestFromStats()
        utest = Mann_Whitney_U_test()
        ztest = Z_test()
        
        cfg.logger.info(f"{metric_items.name}")
        
        df_ = calculate_linearization(calculated_metric)
        #выбор критерия
        if (metric_items.estimator == "t_test"):
            stats = calculate_statistics(df_, metric_items.type)
            criteria_res = ttest(stats)
            report_items = pd.DataFrame({
            "metric_name": metric_items.name,
            "mean_0": stats.mean_0,
            "mean_1": stats.mean_1,
            "var_0": stats.var_0,
            "var_1": stats.var_1,
            "delta": stats.mean_1 - stats.mean_0,
            "lift":  (stats.mean_1 - stats.mean_0) / stats.mean_0,
            "test_name": metric_items.estimator, #добавили - какой тест используется
            "pvalue": criteria_res.pvalue,
            "statistic": criteria_res.statistic
            }, index=[0])
        
        if (metric_items.estimator == "mann_whitney"):
            stats = calculate_statistics(df_, metric_items.type)
            criteria_res = utest(data_by_groups(df_))
            report_items = pd.DataFrame({
            "metric_name": metric_items.name,
            "mean_0": stats.mean_0,
            "mean_1": stats.mean_1,
            "var_0": stats.var_0,
            "var_1": stats.var_1,
            "delta": stats.mean_1 - stats.mean_0,
            "lift":  (stats.mean_1 - stats.mean_0) / stats.mean_0,
            "test_name": metric_items.estimator,
            "pvalue": criteria_res.pvalue,
            "statistic": criteria_res.statistic
            }, index=[0])
        
        if (metric_items.estimator == "prop_test"):
            stats = calculate_statistics(df_, metric_items.type)
            criteria_res = ztest(data_by_groups(df_))
            report_items = pd.DataFrame({
            "metric_name": metric_items.name,
            "mean_0": stats.mean_0,
            "mean_1": stats.mean_1,
            "var_0": stats.var_0,
            "var_1": stats.var_1,
            "delta": stats.mean_1 - stats.mean_0,
            "lift":  (stats.mean_1 - stats.mean_0) / stats.mean_0,
            "test_name": metric_items.estimator,
            "pvalue": criteria_res.pvalue,
            "statistic": criteria_res.statistic
            }, index=[0])
            
        return Report(report_items)


def build_experiment_report(df, metric_config):
    build_metric_report = BuildMetricReport()
    reports = []

    for metric_params in metric_config:
        metric_parsed = Metric(metric_params)
        calculated_metric = CalculateMetric(metric_parsed)(df)
        metric_report = build_metric_report(calculated_metric, metric_parsed)
        reports.append(metric_report.report)

    return pd.concat(reports)

