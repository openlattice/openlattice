pushd ..
./gradlew :shuttle:run -Dexec.args="--create demo@openlattice.com --flight ../scripts/flights/demohealth.yaml --config ../scripts/flights/integration.yaml --environment LOCAL --datasource demodata --token $TOKEN"
popd
