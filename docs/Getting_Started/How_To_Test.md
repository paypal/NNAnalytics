**How To Test:**

0. `git clone https://github.com/paypal/NNAnalytics.git`
1. (Optional) Run `./gradlew -PmainClass=com.paypal.nnanalytics.TestNNAWithStreamEngine execute`. This will run the `public static void main` method in `com.paypal.nnanalytics.TestNNAWithStreamEngine.java` under `src/test/java`. Use CTRL+C to stop the demo.
2. (Optional) Run `./gradlew -PmainClass=com.paypal.nnanalytics.TestWithMiniClusterWithStreamEngine execute`. This will run the `public static void main` method in `com.paypal.nnanalytics.TestWithMiniClusterWithStreamEngine.java` under `src/test/java`. Use CTRL+C to stop the demo.
3. A local instance of NNA should start and be accessible at [http://localhost:4567](http://localhost:4567). Use a browser, preferably Chrome or Firefox, to view. 

** The difference between (a) and (b) is that (a) will launch a static instance of NNA with an unchanging files and directories where as (b) will mimic a production cluster on your local machine and update NNA as it runs.