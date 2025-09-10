* Required components
  * Kafka -> must use at least a broker, other Kafka components (e.g., Connect) are optional
  * stream processor -> Flink (any API) or Spark (structured streaming)
  * web frontend -> must show some live data, any technology is fine
  * any programming language / data source / additional component are OK
    * e.g., Docker (recommended), Kafka producers, backend apps ...
    * a streaming data source is generally required but may authorize re-playing of stored data
    * may reuse sources / architectures / technical solutions of labs, as long as project is original
* Submitted as git repository
  * https://gitlab.inf.unibz.it/ repository (public or private, my username: floriano.zini)
  * must include a README indicating how to run the code (even if not possible, e.g., private data)
  * may indicate the tag/commit to evalute
    * if omitted: last default branch commit made before the submission deadline
