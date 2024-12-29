from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa


class Crypt:

    @staticmethod
    def generate_asymmetrical_keys() -> tuple[bytes, bytes]:
        """Generate a pair of RSA private and public keys.

        Returns:
            tuple[bytes, bytes]: A tuple containing (private_key, public_key) in PEM format
        """
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
        )

        pkcs8_key = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

        # Extract public key (equivalent to openssl rsa -pubout)
        public_key = private_key.public_key()
        public_bytes = public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )

        return pkcs8_key, public_bytes

    @staticmethod
    def load_private_key(private_key: bytes | str) -> rsa.RSAPrivateKey:
        """Load a private key from PEM format.

        Args:
            private_key: The private key in PEM format, either as bytes or string

        Returns:
            RSAPrivateKey: The loaded private key object
        """
        if isinstance(private_key, str):
            private_key = private_key.encode('utf-8')

        return serialization.load_pem_private_key(
            private_key,
            password=None,
        )
