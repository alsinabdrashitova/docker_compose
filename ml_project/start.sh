#!/bin/bash

python3 data_preprocessing/dataprocessor.py
python3 clustering/train.py --noiseLimit=0 --seed=41 --r=1.5
python3 classification/train.py
python3 features/main.py
