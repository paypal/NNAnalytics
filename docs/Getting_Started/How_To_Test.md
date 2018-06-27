**How To Test:**

0. `git clone https://github.com/paypal/NNAnalytics.git`
1. (Temporary measure) Create a directory for NNA to use: `mkdir /usr/local/nn-analytics`.
2. (Optional) Run `./gradlew -PmainClass=TestNNAnalytics execute`. This will run the `public static void main` method in `TestNNAnalytics.java` under `src/test/java`. Use CTRL+C to stop the demo.
3. (Optional) Run `./gradlew -PmainClass=TestWithMiniCluster execute`. This will run the `public static void main` method in `TestWithMiniCluster.java` under `src/test/java`. Use CTRL+C to stop the demo.
4. A local instance of NNA should start and be accessible at [http://localhost:4567](http://localhost:4567). Use a browser, preferably Chrome or Firefox, to view. 

** The difference between (a) and (b) is that (a) will launch a static instance of NNA with an unchanging files and directories where as (b) will mimic a production cluster on your local machine and update NNA as it runs.