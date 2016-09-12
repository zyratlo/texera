from subprocess import call
from subprocess import check_output
import os

# @author Hailey Pan

if __name__ == "__main__":

    home = "/home/bot/textdbworkspace/"
    textdb_home = "textdb/textdb/"
    result_path = "textdb-perftest/data-files/results/"    
    branch = "master"

    textdb_path = home + textdb_home
    result_folder = home + textdb_home + result_path
   

    os.chdir(textdb_path)
    call(["git", "checkout", branch])
    git_update_string = check_output(["git", "pull"]).splitlines()[-1].decode("UTF-8")
    if git_update_string != "Already up-to-date.":
        call(["mvn", "clean"])
        call(["mvn", "test" , "exec:java"])
        git_log_str = check_output(["git", "log"]).split()[1].decode("UTF-8")[:7]
        for file in os.listdir(result_folder):
            if file.endswith(".csv"):
                with open(result_folder + file, "r") as result_file:
                    lines = [line.strip() for line in result_file.readlines() if line.strip()]
                with open(result_folder + file, "w") as result_file:
                    num_of_columns = len(lines[0].split(","))
                    lines = [line+","+"c_"+git_log_str if len(line.split(",")) < num_of_columns else line for line in lines]
                    result_file.write("\n".join(lines))
