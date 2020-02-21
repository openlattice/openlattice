pushd ..
HEALTH_SQL="select * from public.demo_health"
./gradlew :shuttle:run -Dexec.args="--create demo@openlattice.com --flight ../scripts/flights/demohealth.yaml --config ../scripts/flights/integration.yaml --environment LOCAL --datasource demodata --sql \"${HEALTH_SQL}\" --token $TOKEN"
JUSTICE_SQL="select * from public.demo_justice limit 1000"
./gradlew :shuttle:run -Dexec.args="--create demo@openlattice.com --flight ../scripts/flights/demojustice.yaml --config ../scripts/flights/integration.yaml --environment LOCAL --datasource demodata --sql \"${JUSTICE_SQL}\" --token $TOKEN"
popd