### This is a prototype of Samza SQL Runner

This project is dependent on the build of the Samza master in local maven (0.13.0-SNAPSHOT). To build the project, please first checkout Samza master:

    git clone http://git-wip-us.apache.org/repos/asf/samza.git
    cd samza

Then build a package for it:

    ./gradlew publishToMavenLocal

After that, you can build this project by:

    cd ssr
    ./gradlew build idea

Now you can run the examples under test. The examples include:

* **HrSchemaExample:** this example uses an array as table and performs simple "select .. from .." query from it