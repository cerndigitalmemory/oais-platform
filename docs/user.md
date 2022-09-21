# User documentation

This document provides some general information on how to use the CERN Digital Memory platform.

## Setting tokens

Before being able to fetch resources from some sources (e.g. CodiMD, Indico, GitLab,..) you need to configure them first.

To set API and auth tokens, go in your user settings page.

### CodiMD

From your browser, login to the Indico instance, go to "Preferences" and then "API Token". Create new token, name can be anything. Select (at least) Everything (all methods) and Classic API (read only) as scopes. Note down the token and paste it there.

### Indico

To create packages out of CodiMD documents, go to https://codimd.web.cern.ch/, authenticate and after the redirect to the main page open your browser developer tools (CTRL+SHIFT+I), go to the "Storage" tab and under cookies copy the value of the connect.sid cookie. The Record ID for CodiMD document is the part of the url that follows the main domain address (e.g. in https://codimd.web.cern.ch/KabpdG3TTHKOsig2lq8tnw# the recid is KabpdG3TTHKOsig2lq8tnw)

### SSO Comp

TODO
