import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from ....libs.crypt import Crypt


class TestCrypt:
    def test_generate_asymmetrical_keys(self):
        # Arrange
        crypt = Crypt()

        # Act
        private_key_bytes, public_key_bytes = crypt.generate_asymmetrical_keys()

        # Assert
        # Check that both keys are bytes
        assert isinstance(private_key_bytes, bytes)
        assert isinstance(public_key_bytes, bytes)

        # Verify we can load the private key
        private_key = serialization.load_pem_private_key(
            private_key_bytes,
            password=None,
        )
        assert isinstance(private_key, rsa.RSAPrivateKey)

        # Verify we can load the public key
        public_key = serialization.load_pem_public_key(
            public_key_bytes
        )
        assert isinstance(public_key, rsa.RSAPublicKey)

        # Verify the key size is 2048
        assert private_key.key_size == 2048
        assert public_key.key_size == 2048
