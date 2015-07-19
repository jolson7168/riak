export PYTHONPATH=../src 
export quoted="\"$1\""
python ../test/executeCoverageTC.py --file ../config/coverageTestCases_config --comment $quoted
