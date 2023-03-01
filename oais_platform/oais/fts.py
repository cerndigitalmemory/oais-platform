import logging

import fts3.rest.client.easy as fts3


class FTS:
    def __init__(self, fts_instance, user_cert_path, cert_key_path):
        logging.debug(
            f"Authenticating to FTS instance {fts_instance} using {user_cert_path} / {cert_key_path}"
        )
        # Login to FTS and set up the context
        context = fts3.Context(
            fts_instance,
            ucert=user_cert_path,
            ukey=cert_key_path,
            verify=False,
        )
        logging.info(
            f'Authenticated on FTS with certificate DN: { fts3.whoami(context)["user_dn"] } '
        )
        self.context = context

    def prepare_push_job(self, source, dest, timeout=604800):
        return {
            "files": [
                {
                    "sources": [
                        source,
                    ],
                    "destinations": [dest],
                }
            ],
            "params": {"archive_timeout": timeout},
        }

    def prepare_retrieve_job(self, source, dest, bring_online=259200):
        """
        CTA administrators recommend to set bring_online to 72 hours (259200 seconds).
        It would allow the transfers to keep running over the weekend, and in case of a problem
        the tape operators would be able to check it on Monday
        """
        return {
            "files": [
                {
                    "sources": [
                        source,
                    ],
                    "destinations": [dest],
                }
            ],
            "params": {"bring_online": bring_online},
        }

    def push_to_cta(self, source, dest):
        job = self.prepare_push_job(source, dest)
        submitted_job = fts3.submit(job=job, context=self.context)
        return submitted_job

    def retrieve_from_cta(self, source, dest):
        job = self.prepare_retrieve_job(source, dest)
        submitted_job = fts3.submit(job=job, context=self.context)
        return submitted_job

    def job_status(self, job_id):
        return fts3.get_job_status(self.context, job_id, list_files=False)
