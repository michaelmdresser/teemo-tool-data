(defproject teemo-tool-data "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "none"
            :url "none"}
  :dependencies [[org.clojure/clojure "1.10.1"]]
  :main ^:skip-aot teemo-tool-data.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}})
