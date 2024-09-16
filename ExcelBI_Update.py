import subprocess
import os
import sys

def run_script(script_path, script_name):
    try:
        subprocess.run(['python', script_path], check=True)
        print(f"{script_name} executed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error executing {script_name}: {e}")

if __name__ == "__main__":
    # Script directory
    script_directory = os.path.dirname(os.path.abspath(sys.argv[0]))

    # Specify the names of your Python scripts
    script_names = ['sumAccessMarkerMachine.py', 'weeklyChange_rev.py']

    # Call each script
    for script_name in script_names:
        script_path = os.path.join(script_directory, script_name)
        run_script(script_path, script_name)
