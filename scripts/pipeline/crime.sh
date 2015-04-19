# Run a Ruffus pipeline using the CRIME example
echo pipeline start: `date`
module=prdbsens/pipeline/crime
cfg=config/pipeline/crime.yaml
lfile=logs/pipeline/crime.txt
threads=3
#verbose=4
verbose=1
extra=$1
cmd="python -m $module -v$verbose --config $cfg -j $threads -L $lfile $extra"
echo $cmd
$cmd >> $lfile 2>&1
echo pipeline done: `date`
