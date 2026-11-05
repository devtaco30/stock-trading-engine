// 발신기(U10Tool)를 gradle 없이 java 한 번으로 실행하기 위해, u10Tool 작업이 쓰는 클래스패스를 폴더 하나로 모은다.
// u10Tool 작업(account-worker/build.gradle.kts)의 classpath는 sourceSets["test"].runtimeClasspath이고 메인 클래스는 U10Tool이다.
// 빌드 파일을 고치지 않으려고 init 스크립트로 작업을 더한다. 사용(워크트리 루트에서):
//   ./gradlew --offline -q -I k8s/sender/u10tool-classpath.init.gradle.kts :account-worker:exportU10ToolClasspath
// 결과: account-worker/build/u10tool/classes(클래스·리소스 폴더를 합친 것), account-worker/build/u10tool/lib(jar)
// 실행: java --add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
//         -cp 'account-worker/build/u10tool/classes:account-worker/build/u10tool/lib/*' com.flab.stocktradingengine.account.worker.u10.U10Tool <명령>
allprojects {
    if (name == "account-worker") {
        afterEvaluate {
            val sourceSets = extensions.getByType(org.gradle.api.tasks.SourceSetContainer::class.java)
            val testRuntime = sourceSets.getByName("test").runtimeClasspath
            tasks.register("exportU10ToolClasspath", Sync::class.java) {
                dependsOn(tasks.named("testClasses"))
                into(layout.buildDirectory.dir("u10tool"))
                from(provider { testRuntime.files.filter { it.isDirectory } }) {
                    into("classes")
                }
                from(provider { testRuntime.files.filter { it.isFile && it.name.endsWith(".jar") } }) {
                    into("lib")
                }
            }
        }
    }
}
