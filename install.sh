#!/bin/bash

# 1. First check to see if the correct Python version is installed in the system
echo "1. Checking Python version..."
REQ_PYTHON_V="360"

ACTUAL_PYTHON_V=$(python -c 'import sys; version=sys.version_info[:3]; print("{0}{1}{2}".format(*version))')
ACTUAL_PYTHON3_V=$(python3 -c 'import sys; version=sys.version_info[:3]; print("{0}{1}{2}".format(*version))')


if [[ $ACTUAL_PYTHON_V > $REQ_PYTHON_V ]] || [[ $ACTUAL_PYTHON_V == $REQ_PYTHON_V ]];  then 
    PYTHON="python"
elif [[ $ACTUAL_PYTHON3_V > $REQ_PYTHON_V ]] || [[ $ACTUAL_PYTHON3_V == $REQ_PYTHON_V ]]; then 
    PYTHON="python3"
else
    echo -e "\tPython 3.6 is not installed on this machine. Please install Python 3.6 or greater before continuing."
    exit 1
fi

echo -e "\t--Python 3.6 or greater is installed"

# 2. What OS are we running on?
PLATFORM=$($PYTHON -c 'import platform; print(platform.system())')

echo -e "2. Checking OS Platform..."
echo -e "\t--OS=Platform=$PLATFORM"

# 3. Create Virtual environment 
echo -e "3. Creating new virtual environment..."

# Remove the env directory if it exists 
if [[ -d env ]]; then 
    echo -e "\t--Virtual Environment already exists. Deleting old one now."
    rm -rf env 
fi

$PYTHON -m venv env
if [[ ! -d env ]]; then 
    echo -e "\t--Could not create virutal environment...Please make sure venv is installed"
    exit 1
fi

# 4. Install Requirements 

echo -e "4. Installing Requirements..."
if [[ ! -e "requirements.txt" ]]; then 
    echo -e "\t--Need to requirements.txt to install packages."
    exit 1
fi


if [[ "$PLATFORM" == "Windows" ]]; then
    echo -e "Working with $PLATFORM"
    echo -e "\t--Activating environment in $PLATFORM"
    source env/scripts/activate
else
    echo -e "\t--Activating environment in $PLATFORM"
    source env/bin/activate
fi

pip install -r requirements.txt
echo -e "Install is complete."

read -p "`echo $'\t'` Please provide where do you want to access raw data from:`echo $'\n \t'` 1. 'api' `echo $'\n \t'` 2. 'disk' `echo $'\n '` Enter 1 or 2: " MODE
echo -e $MODE
$PYTHON clean_data.py $MODE
read -p "`echo $'\t'` Please provide the maximum walking distance (in km; Acceptable range: 0.1 to 2): " MAXWALKINGDIST
$PYTHON clusteringv1.py $MAXWALKINGDIST
echo -e "The distance matrix generation will take about 1 hour..."
# $PYTHON generate_matrix.py
echo -e "Route optimization will take about 10 minutes..."
$PYTHON route_optimization.py
$PYTHON generate_map.py
if [[ "$PLATFORM" == "Linux" ]]; then
    xdg-open m.html
elif [[ "$PLATFORM" == "Windows" ]]; then
    start m.html
else
    open m.html
fi

deactivate


