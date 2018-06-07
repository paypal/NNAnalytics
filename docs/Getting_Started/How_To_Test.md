**How To Test:**

0. `git clone https://github.com/paypal/NNAnalytics.git`
1. (Temporary measure) Create a directory for NNA to use: `mkdir /usr/local/nn-analytics`.
2. (a) Run the `public static void main` method in `TestNNAnalytics.java` under `src/test/java`.
2. (b) Run the JUnit test `testAddFiles()` in `TestWithMiniCluster.java` under `src/test/java`.
3. A local instance of NNA should start and be accessible at http://localhost:4567.