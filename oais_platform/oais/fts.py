import logging
from datetime import datetime, timezone

import fts3.rest.client.easy as fts3
from cryptography import x509


class FTS:
    archive_timeout = 86400
    copy_pin_lifetime = -1
    cert_ttl_days_error = 30

    def __init__(self, fts_instance, user_cert_path, cert_key_path):
        logging.debug(
            f"Authenticating to FTS instance {fts_instance} using {user_cert_path} / {cert_key_path}"
        )
        logging.getLogger("fts3.rest.client").setLevel(logging.DEBUG)

        # Login to FTS and set up the context
        context = fts3.Context(
            fts_instance,
            ucert=user_cert_path,
            ukey=cert_key_path,
            verify=True,
        )
        logging.info(
            f'Authenticated on FTS with certificate DN: { fts3.whoami(context)["user_dn"] } '
        )
        self.context = context
        self.cert_path = user_cert_path
        self.check_ttl()

    def check_ttl(self):
        """Get the time to live of a certificate. If it is below the threshold, send an error."""
        logging.debug(f"Checking the ttl of the certificate {self.cert_path}")

        # Load certificate from a file (PEM format)
        with open(self.cert_path, "rb") as f:
            cert_data = f.read()

        cert = x509.load_pem_x509_certificate(cert_data)

        # Get expiration date
        expiry_date = cert.not_valid_after_utc
        now = datetime.now(timezone.utc)
        ttl_days = (expiry_date - now).days
        logging.debug(f"The certificate is valid for {ttl_days} days")
        if ttl_days < self.cert_ttl_days_error:
            logging.error(
                f"The certificate {self.cert_path} is going to expire in {ttl_days} (which is smaller than {self.cert_ttl_days_error} days)"
            )

    def push_to_cta(self, source, dest, overwrite=False):
        logging.info(f"Starting FTS transfer from {source} to {dest}.")
        transfer = fts3.new_transfer(source, dest)
        job = fts3.new_job(
            [transfer],
            verify_checksum=True,
            metadata="Digital Memory job",
            retry=1,
            priority=3,
            archive_timeout=self.archive_timeout,
            copy_pin_lifetime=self.copy_pin_lifetime,
            overwrite=overwrite,
        )
        self.check_ttl()
        submitted_job = fts3.submit(job=job, context=self.context)
        return submitted_job

    def job_status(self, job_id):
        return fts3.get_job_status(self.context, job_id, list_files=True)

    def number_of_transfers(self):
        return len(fts3.list_jobs(self.context))

    def delegate(self):
        logging.info("Delegating certificate")
        fts3.delegate(self.context, force=True)
