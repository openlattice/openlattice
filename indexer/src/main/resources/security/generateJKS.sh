read -p "Enter subject alternative name[localhost]: " san
if [ "$san" = "" ]; then
    san='localhost'
fi
echo "Using $san"
keytool -keystore rhizome.jks -delete -alias rhizomessl
keytool -genkeypair -keyalg EC -keystore rhizome.jks -alias rhizomessl -keypass rhizome -dname "CN=$san, OU=Core Platform, O=OpenLattice, L=Redwood City, S=CA, C=US" -validity 720 
keytool -keystore rhizome.jks -exportcert -alias rhizomessl -file rhizome_ssl.cer
