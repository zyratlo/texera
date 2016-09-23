from subprocess import call
from subprocess import check_output
import os

# author Hailey Pan and Zuozhi Wang

# This Python script is for running TextDB performance test automatically.
# It will:
# pull the latest changes from github
# if there's a change in master branch, run the performance test
# append the commit number to performance test results

textdb_workspace = "/home/bot/textdbworkspace/"
maven_repo_home = "/home/bot/.m2/repository/"
java8_bin = "/usr/bin/java"

textdb_home = "textdb/textdb/"
result_path = "textdb-perftest/perftest-files/results/"
branch = "master"
main_class = "edu.uci.ics.textdb.perftest.runme.RunTests"

textdb_path = textdb_workspace + textdb_home
textdb_perftest_path = textdb_path + "textdb-perftest/"
result_folder = textdb_workspace + textdb_home + result_path

perftest_arguments = ["/home/bot/textdbworkspace/data-files/", "\"\"","\"\"","\"\"","\"\""]


def build_run_command():
    command = "" + \
        java8_bin + " " + \
        "-Dfile.encoding=UTF-8 -classpath" + " " + \
        textdb_workspace + "/textdb/textdb/textdb-perftest/target/classes" + ":" + \
        maven_repo_home + "junit/junit/4.8.1/junit-4.8.1.jar" + ":" + \
        maven_repo_home + "org/apache/lucene/lucene-core/5.5.0/lucene-core-5.5.0.jar" + ":" + \
        maven_repo_home + "org/apache/lucene/lucene-analyzers-common/5.5.0/lucene-analyzers-common-5.5.0.jar" + ":" + \
        maven_repo_home + "org/apache/lucene/lucene-queryparser/5.5.0/lucene-queryparser-5.5.0.jar" + ":" + \
        maven_repo_home + "org/apache/lucene/lucene-queries/5.5.0/lucene-queries-5.5.0.jar" + ":" + \
        maven_repo_home + "org/apache/lucene/lucene-sandbox/5.5.0/lucene-sandbox-5.5.0.jar" + ":" + \
        maven_repo_home + "org/json/json/20160212/json-20160212.jar" + ":" + \
        textdb_workspace + "/textdb/textdb/textdb-api/target/classes" + ":" + \
        textdb_workspace + "/textdb/textdb/textdb-common/target/classes" + ":" + \
        textdb_workspace + "/textdb/textdb/textdb-dataflow/target/classes" + ":" + \
        maven_repo_home + "com/google/re2j/re2j/1.1/re2j-1.1.jar" + ":" + \
        textdb_workspace + "/textdb/textdb/textdb-storage/target/classes" + ":" + \
        maven_repo_home + "edu/stanford/nlp/stanford-corenlp/3.6.0/stanford-corenlp-3.6.0.jar" + ":" + \
        maven_repo_home + "com/io7m/xom/xom/1.2.10/xom-1.2.10.jar" + ":" + \
        maven_repo_home + "xml-apis/xml-apis/1.3.03/xml-apis-1.3.03.jar" + ":" + \
        maven_repo_home + "xerces/xercesImpl/2.8.0/xercesImpl-2.8.0.jar" + ":" + \
        maven_repo_home + "xalan/xalan/2.7.0/xalan-2.7.0.jar" + ":" + \
        maven_repo_home + "joda-time/joda-time/2.9/joda-time-2.9.jar" + ":" + \
        maven_repo_home + "de/jollyday/jollyday/0.4.7/jollyday-0.4.7.jar" + ":" + \
        maven_repo_home + "javax/xml/bind/jaxb-api/2.2.7/jaxb-api-2.2.7.jar" + ":" + \
        maven_repo_home + "com/googlecode/efficient-java-matrix-library/ejml/0.23/ejml-0.23.jar" + ":" + \
        maven_repo_home + "javax/json/javax.json-api/1.0/javax.json-api-1.0.jar" + ":" + \
        maven_repo_home + "org/slf4j/slf4j-api/1.7.12/slf4j-api-1.7.12.jar" + ":" + \
        maven_repo_home + "edu/stanford/nlp/stanford-corenlp/3.6.0/stanford-corenlp-3.6.0-models.jar" + ":" + \
        maven_repo_home + "org/mockito/mockito-all/1.9.5/mockito-all-1.9.5.jar" + " " + \
        main_class + " " +\
        " ".join(perftest_arguments)
    return command



if __name__ == "__main__":

    os.chdir(textdb_path)
    call(["git", "checkout", branch])
    git_update_string = check_output(["git", "pull"]).splitlines()[-1].decode("UTF-8")
    if git_update_string != "Already up-to-date.":
        call(["mvn", "clean", "install"])

        os.chdir(textdb_perftest_path)
        call(build_run_command(), shell = True)

        git_log_str = check_output(["git", "log"]).split()[1].decode("UTF-8")[:7]
        for file in os.listdir(result_folder):
            if file.endswith(".csv"):
                with open(result_folder + file, "r") as result_file:
                    lines = [line.strip() for line in result_file.readlines() if line.strip()]
                with open(result_folder + file, "w") as result_file:
                    num_of_columns = len(lines[0].split(","))
                    lines = [line+","+"c_"+git_log_str if len(line.split(",")) < num_of_columns else line for line in lines]
                    result_file.write("\n".join(lines))
