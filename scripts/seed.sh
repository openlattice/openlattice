pushd ..
SQL="select * from public.demo_health"
./gradlew :shuttle:run -Dexec.args="--create demo@openlattice.com --flight ../scripts/flights/demohealth.yaml --config ../scripts/flights/integration.yaml --environment LOCAL --datasource demodata --sql \"${SQL}\" --token $TOKEN"
popd