import subprocess

# Specify the paths to your Python scripts
script1_path = 'sumAccessMarkerMachinePowerBI.py'
script2_path = 'weeklyChange_rev_PowerBI.py'

# Call the first script
subprocess.call(['python', script1_path])

# Call the second script
subprocess.call(['python', script2_path])
