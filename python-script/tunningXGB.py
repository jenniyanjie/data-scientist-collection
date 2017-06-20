# -*- coding: utf-8 -*-
"""
Created on Tue Apr 18 10:55:09 2017
to tunning xgboost model
@author: Jennifer
"""
from __future__ import print_function 
import os, sys, random, logging
from time import time
from datetime import datetime
import pandas as pd
import numpy as np

from xgboost import XGBClassifier
import xgboost as xgb
from sklearn import datasets
from sklearn.model_selection import GridSearchCV, StratifiedKFold, cross_val_score
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import OneHotEncoder

import matplotlib
matplotlib.use('Agg')
import matplotlib.pylab as plt
from matplotlib.pylab import rcParams
rcParams['figure.figsize'] = 12, 4
        
print(__doc__)

def setup_logging(default_level=logging.WARNING):
    """
    Setup logging configuration
    """
    logging.basicConfig(format='%(asctime)s   %(message)s', level=default_level)
    return logging.getLogger('Check_ifa_existence')


def estimate_xgb_nround(xgbClassifier, X, y, cvfold=3 ):
    """
    The 'learning_rate' and 'n_estimators' yields a precision-time tradeoff 
    to xgb modelling.
    In practical setting, if the best n_estimators too big (say larger than 200..),
    the tunning work would be too time-consuming. decrease the learning-rate would
    yield a smaller n_estimators, thus more efficiency in future tunning.
    
    Here, we take the benefit of the early_stop function of xgb.cv to estimate
    the best n_estimators
    
    input: 
        xgbClassifier: scikit-learn API for XGBoost classification, do set the 
                        n_estimators large enough (larger than bestStep)
        X: feature matrix
        y: labels
    return:
        bestStep
    """
    xgbParam = xgbClassifier.get_xgb_params()
    
    nClasses = np.unique(y).size
    xgbParam["num_class"] = nClasses # model does not set the num_class
    
    xgtrain = xgb.DMatrix(X, label=y)
    cvresult = xgb.cv(xgbParam, 
                      xgtrain, 
                      num_boost_round=xgbClassifier.get_params()['n_estimators'], 
                      nfold=cvfold, 
                      metrics='mlogloss', 
                      early_stopping_rounds=50, 
                      verbose_eval=50)
    bestStep = cvresult.shape[0]
    print("model stop at step {}".format(bestStep))            
    return bestStep

def plot_importance_matrix(xgbClassifier, fig_path):
    featImp = pd.Series(xgbClassifier.booster().get_fscore())[:100].sort_values(ascending=False)
    featImp.plot(kind='bar', title='Feature Importances')
    plt.ylabel('Feature Importance Score') 
    figNm = "feature_importance_{}.png".format(datetime.now().strftime("%Y-%m-%d-%H-%M-%S"))
    plt.savefig(os.path.join(fig_path, figNm))
    return

##############################################################################
# load data #################################################################
##############################################################################
logger = setup_logging(logging.INFO) 

# import some data to play with
iris = datasets.load_iris()
X = iris.data
y = iris.target

random.seed(2016)

##############################################################################
# grid search ################################################################
##############################################################################
if __name__ == "__main__":
    xgb_model = XGBClassifier(      
                             learning_rate=0.4, # eta
                             n_estimators=167,
                             max_depth=5,
                             min_child_weight=1,
                             gamma=0.2,
                             subsample=0.9,
                             colsample_bytree=0.7,
                             objective='multi:softprob',
                             scale_pos_weight=1,
                             seed=0,
                             )
    
    xgb_enc = OneHotEncoder(handle_unknown='ignore')    
    xgb_enc.fit(X) # since I am working mostly on categorical features
    
    estimate_nround = False
    if estimate_nround: 
        logger.info('estimating the n_estimators...')
        best_n_rounds = estimate_xgb_nround(xgb_model, X, y)
        logger.info('complete estimating the n_estimators')
        xgb_model.set_params(n_estimators=best_n_rounds)
        xgb_model.fit(xgb_enc.transform(X), y)
        plot_importance_matrix(xgb_model, csv_path)
        sys.exit()
        
    # start tunning
    param_grid = {
                ### step 1 ###
#                'max_depth': [3, 5, 7, 9],
#                'min_child_weight': [1, 3, 5]
                ### best parameter for round 1: max_depth = 5, min_child_weight = 1 ###
                ### step 2 ###
#                'max_depth': [4, 5, 6],
#                'min_child_weight': [1, 2]    
                ### best parameter for round 2: max_depth = 5, min_child_weight = 1 ###
                ### step 3 ###
#                'gamma': [0.0, 0.1, 0.2, 0.3, 0.4]
                ### best parameter for round 3: gamma = 0.2 ###
                ### step 4 ###                
#                 'subsample':[0.6, 0.7, 0.8, 0.9], 
#                 'colsample_bytree':[0.6, 0.7, 0.8, 0.9]
                ### best parameter for round 4: subsample = 0.9, colsample_bytree = 0.7 ###
                ### step 5 ### 
#                 'reg_alpha':[1e-5, 1e-2, 0.1, 1, 100], 
#                 'reg_alpha':[0, 0.001, 0.005, 0.01, 0.05]
                ### best parameter for round 5: reg_alpha = 1e-5 ###
                ### step 6 ### 
                 'reg_lambda':[0, 0.001, 0.005, 0.01, 0.05]
                ### best parameter for round 6: reg_lambda = 0.005 ###
                 } 
        
    
    kfold = StratifiedKFold(n_splits=3, shuffle=True, random_state=0)
    grid_search = GridSearchCV(xgb_model, param_grid, scoring="neg_log_loss", 
                               n_jobs=-1, cv=kfold, verbose=3,
                               error_score=-99)
    t0 = time()
    logger.info("Performing grid search...")
    grid_result = grid_search.fit(xgb_enc.transform(X), y)
    t1 = time()
    logger.info("Grid search done in %0.3fs.\n" % (t1 - t0)) 
    
    nested_cv = False
    if nested_cv:
        nested_score = cross_val_score(grid_search, X=xgb_enc.transform(X), y=y, cv=kfold)
        nested_cv_score = nested_score.mean()
        t2 = time()
        logger.info("Nested cv done in %0.3fs.\n" % (t2 - t1)) 
        
    # summarize results
    logger.info("Summary:")
    print("Best score: %0.6f" % grid_search.best_score_)
    print("Best parameters set:")
    best_parameters = grid_search.best_estimator_.get_params()
    for param_name in sorted(param_grid.keys()):
        print("\t%s: %r" % (param_name, best_parameters[param_name]))
    print("--------------------------------------------------------")
    means = grid_result.cv_results_['mean_test_score']
    stds = grid_result.cv_results_['std_test_score']
    params = grid_result.cv_results_['params']
    for mean, stdev, param in zip(means, stds, params):
    	print("%f (%f) with: %r" % (mean, stdev, param))
        