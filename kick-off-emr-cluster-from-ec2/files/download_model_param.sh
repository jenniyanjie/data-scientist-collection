#!/bin/bash -xe

# Parse arguments
intelligent_model_folder="$1"
file_array=("best_iter.pkl" "features.pkl" "labelencoder.pkl" "xgb.model");
for f in "${file_array[@]}"; do
    model_param_obj="$intelligent_model_folder/$f"
    target_script="/home/hadoop/$f"
    aws s3 cp $model_param_obj $target_script
done