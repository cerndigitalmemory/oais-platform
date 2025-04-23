import logging

import fts3.rest.client.easy as fts3


class FTS:
    archive_timeout = 86400
    copy_pin_lifetime = -1

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

    def push_to_cta(self, source, dest):
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
        )

        submitted_job = fts3.submit(job=job, context=self.context)
        return submitted_job

    def job_status(self, job_id):
        return fts3.get_job_status(self.context, job_id, list_files=False)

    def number_of_transfers(self):
        return len(fts3.list_jobs(self.context))

    def delegate(self):
        logging.info("Delegating certificate")
        fts3.delegate(self.context, force=True)
