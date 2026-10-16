"""Generate temporary test PKI and run an encrypted, authenticated OPC UA fixture."""
import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
import socket
import sys
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID, ExtendedKeyUsageOID
from asyncua import Server, ua
from asyncua.server.user_managers import User, UserRole


def certificate(root, name, uri):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)])
    now = datetime.now(timezone.utc)
    cert = (x509.CertificateBuilder().subject_name(subject).issuer_name(subject).public_key(key.public_key())
        .serial_number(x509.random_serial_number()).not_valid_before(now-timedelta(minutes=5)).not_valid_after(now+timedelta(days=2))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(x509.SubjectAlternativeName([x509.UniformResourceIdentifier(uri),x509.DNSName("localhost"),x509.DNSName(socket.gethostname()),x509.IPAddress(__import__('ipaddress').ip_address('127.0.0.1'))]), critical=False)
        .add_extension(x509.KeyUsage(digital_signature=True,content_commitment=True,key_encipherment=True,data_encipherment=True,key_agreement=False,key_cert_sign=False,crl_sign=False,encipher_only=None,decipher_only=None), critical=True)
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH,ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
        .sign(key,hashes.SHA256()))
    (root/f'{name}.der').write_bytes(cert.public_bytes(serialization.Encoding.DER))
    (root/f'{name}.pem').write_bytes(key.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.TraditionalOpenSSL,serialization.NoEncryption()))
    (root/f'{name}.pem').chmod(0o600)
    return cert


class Users:
    def get_user(self, isession, username=None, password=None, certificate=None):
        if username == 'fixture-user' and password == 'fixture-password':
            return User(role=UserRole.User, name=username)
        # X509 user identity is tested separately from secure-channel authentication.
        if certificate and username is None:
            return User(role=UserRole.User, name='certificate-user')
        return None


async def main(root):
    root.mkdir(parents=True,exist_ok=True)
    server_cert = certificate(root,'server','urn:moqui:secure:fixture')
    client_cert = certificate(root,'client','urn:moqui:device:gateway')
    certificate(root,'untrusted-client','urn:moqui:device:gateway')
    for directory in ['trusted','rejected']:
        (root/'client-pki'/directory).mkdir(parents=True,exist_ok=True)
    thumbprint = server_cert.fingerprint(hashes.SHA1()).hex()
    (root/'client-pki'/'trusted'/f'CN=server [{thumbprint}].der').write_bytes((root/'server.der').read_bytes())
    server = Server(user_manager=Users())
    await server.init()
    await server.set_application_uri('urn:moqui:secure:fixture')
    server.set_endpoint('opc.tcp://127.0.0.1:12687/secure')
    await server.load_certificate(root/'server.der')
    await server.load_private_key(root/'server.pem')
    server.set_security_policy([ua.SecurityPolicyType.Basic256Sha256_Sign,ua.SecurityPolicyType.Basic256Sha256_SignAndEncrypt,
        ua.SecurityPolicyType.Aes128Sha256RsaOaep_SignAndEncrypt,ua.SecurityPolicyType.Aes256Sha256RsaPss_SignAndEncrypt])
    server.set_identity_tokens([ua.UserNameIdentityToken,ua.X509IdentityToken])
    async def validate(cert, application):
        if cert.fingerprint(hashes.SHA256()) != client_cert.fingerprint(hashes.SHA256()):
            raise ua.UaStatusCodeError(ua.StatusCodes.BadCertificateUntrusted)
    server.set_certificate_validator(validate)
    namespace = await server.register_namespace('urn:moqui:gateway:regression')
    value = await server.nodes.objects.add_variable(ua.NodeId('review_value',namespace),'ReviewValue',300.03)
    await value.set_writable()
    async with server:
        print('Secure OPC UA fixture ready',flush=True)
        await asyncio.Event().wait()


if __name__=='__main__':
    asyncio.run(main(Path(sys.argv[1])))
