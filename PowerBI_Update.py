import subprocess

# Specify the paths to your Python scripts
script1_path = 'getIVRCompanyInfo.py'
script2_path = 'getMazricaInfoCustomer.py'

# Call the first script
subprocess.call(['python', script1_path])

# Call the second script
subprocess.call(['python', script2_path])
