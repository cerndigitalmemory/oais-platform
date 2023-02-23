# User documentation

This section provides some general information on how to use the CERN Digital Memory platform. It is targeted at CERN population (service managers, developers, end users) interested in making use of the platform to prepare their personal or institutional data for long-term preservation.

If you instead need to develop on the platform and deploy it locally, check the links in the [onboarding](onboarding.md) section.

> The information presented here is meant to be a informal introduction to some digital preservation concepts and how the CERN Digital Memory platform can be used. More precise definitions and specifications can be found in the [OAIS reference model](https://public.ccsds.org/pubs/650x0m2.pdf) and in the References chapter.

## Why?

Your data is already backed-up and your digital repository is not expected to be dimissioned in the next years. Why should you care about the CERN Digital Memory platform and considering using it?

### Backups vs Digital Preservation

A backup is a short-term data recovery solution following loss or corruption and is **fundamentally different** to an electronic preservation archive. Digital preservation goes far beyond the concept of backups or a mere technical storage solution. Digital preservation combines **policies, strategies and actions** to ensure the most accurate rendering possible of authenticated content over time, regardless of the challenges of file corruption, media failure and technological (hardware and software) change. Digital preservation applies to content that is born digital or converted to digital form. The purpose of preserving digital materials is to maintain accessibility: the ability to access their essential, authentic message or purpose.

See also:

- [Why digital preservation matters](https://www.dpconline.org/handbook/digital-preservation/why-digital-preservation-matters)

### Features of the Digital Memory platform

In more practical terms, this is what the platform can do for you.

(TODO)

If your data is not exported yet:

- Export it in a consistent format from a number of different sources. E.g.:
    - GitLab repositories (including discussions, issues, threads, source code, releases, etc)
    - CERN Digital Repositories such as Indico, CDS, Zenodo, ...
- Export from services where data is not published/cannot be exported (e.g. CodiMD)
- Create "exports" from your local filesystem (or e.g. from CERNBox)
- Provide examples of "export" mechanisms you can adapt to your needs
- Prepare and curate your personal archive of documents scattered around different digital repositories while working at CERN

If already have a way to export your data, by submitting it to the platform you will be able to:

- Get data in a form that can be _disseminated_ and is able to live on its own, without the need of any digital repository or additional software
- Push the preservation versions of important institutional data to the CERN Tape Archive (and not only byte level)
- Get your files automatically converted into preservation formats. E.g.:
    - AC3, AIFF, MP3, WAV, WMA into WAVE
    - AVI, FLV, MOV, MPEG-1, MPEG-2, MPEG-4, SWF, WMV into FFV1/LPCM in MKV
    - PDF to PDF/A
    - BMP, GIF, JPG, JP2*, PCT, PNG*, PSD, TIFF, TGA to TIFF
- Make your data discover-able and search-able on the Digital Memory Registry

## Preliminar definitions

Some important distinctions to make before digging in:

#### SIP, AIP, DIP

- A Submission Information Package (SIP) is a "folder" containing all the digital objects of a Resource, its metadata and some additional information. Our SIP specification can be found [here](https://gitlab.cern.ch/digitalmemory/sip-spec). You can easily create yourself SIPs of your data or use our tool to do it, which supports local folders and some CERN digital repository. It can be easily expanded or used as a reference.
- SIPs are transformed by the platform into Archival Information Packages (AIPs) for preservation. This process includes (but not limited to) converting and re-encoding data and metadata formats according to a specific preservation strategies. Such process is handled by Archivematica.
- Finally, packages are distributed in the form of Dissemination Information Package (DIP).

#### Resource

With the term "Resource" we refer to the set of digital objects grouped under the same identifier (ID) in a specific digital repository.

Resources are defined with the couple (Digital Repository, ID).

Examples of Resources are:

- [https://cds.cern.ch/record/2847763](https://cds.cern.ch/record/2847763)
- [https://indico.cern.ch/event/550015](https://indico.cern.ch/event/550015)

#### Archive

The entire archival _process_ of a Resource is called "Archive" in our platform. Every time a resource is submitted to the platform an Archive is created and the archival process starts.
By browsing the Archive details on the platform you will see which preservation steps are being executed on the pipeline and with which results.

An Archive is generally tied to a Resource. More than one Archive of the same resource can be in the platform. This can happen in the following cases:

- The same resource is archived again but the SIP data is different. E.g. because it was updated upstream or because the SIP was created in a different way.
- The same resource is archived again and the SIP data is the same. This may be desiderable if a different archival strategy is wanted.

In any case, the platform (and the registry) provide mechanisms to discover all the Archives related to a Resource.

#### Registry

The Registry of the Digital Memory platform is a digital repository based on InvenioRDM. The registry is used to _publish_ Archives (and their related preservation assets) and make them searchable and discoverable.

An Archive can be published in different points in time to the Registry and they can differ in what assets they have available (e.g. an Archive can be published when little metadata was extracted and only the SIP was available, and later re-published with more metadata and a AIP). Different Archives, all referring to the same Resource can also be published.

Those "versions" are always grouped under the same "Parent ID" so you will always be able to browse different versions of the same Archive.

#### Artifact

Sometimes, a preservation step in the platform has some assets attached to it. E.g. the "Harvest" step will have the SIP artifact attached to it. You can find additional details and download links for the Artifacts from the Archive details in the platform and on the published Registry entry.

## UI

The simplest way to access the platform features is from the UI located at [preserve.web.cern.ch](https://preserve.web.cern.ch/).

## API

All the functionalities of the platform are exposed by a RESTful API too. The API surface is fully documented by the [OpenAPI specification](https://preserve-qa.web.cern.ch/api/schema/swagger-ui/), refer to the Swagger UI for a detailed overview on how every route should be called.

This section will provide some basic API examples on how to submit data to the platform and retrieve assets. Such steps can easily be automated and hooked to your systems pipelines.

### Creating a SIP

The first thing you need is a way to "package" your data into Submission packages. Those are the packages you will submit to the platform so a preservation pipeline can be started from them.

Imagine that the resource you want to archive consists in a PDF file (e.g. `thesis.pdf`) and some related metadata (`metadata.xml`) exported from your system.

An SIP is actually just a plain folder hosting these two files plus a "manifest", containing some additional information about how you created the package and what upstream resource it refers to.

```
your-sip
├── bag-info.txt
├── bagit.txt
├── data
│   ├── content
│   │   ├── thesis.pdf
│   │   └── metadata.xml
│   └── meta
│       └── sip.json
└── manifest-md5.txt
```

#### The SIP manifest

See [https://gitlab.cern.ch/digitalmemory/sip-spec#sipjson](https://gitlab.cern.ch/digitalmemory/sip-spec#sipjson)


### Uploading a SIP

```bash
curl -X 'POST' \
  -F ‘data=@path/to/local/file.zip’ \
  'https://preserve.web.cern.ch/api/upload/sip'
```

### Asking the platform to Harvest a Resource for you

E.g. for CDS record 2728246:

```bash
curl -X 'POST' \
  'https://preserve.web.cern.ch/api/archives/create/harvest/' \
  -d '{
    "source": "CDS",
    "recid": "2728246"
  }'
```

Some sources may require additional configuration.

## Configuration

Before being able to fetch resources from some sources (e.g. CodiMD, Indico, GitLab,..) you need to configure them first.

To set API and auth tokens, go in your user settings page.

### CodiMD

From your browser, login to the Indico instance, go to "Preferences" and then "API Token". Create new token, name can be anything. Select (at least) Everything (all methods) and Classic API (read only) as scopes. Note down the token and paste it there.

### Indico

To create packages out of CodiMD documents, go to https://codimd.web.cern.ch/, authenticate and after the redirect to the main page open your browser developer tools (<kbd>CTRL</kbd>+<kbd>SHIFT</kbd>+<kbd>I</kbd>), go to the "Storage" tab and under cookies copy the value of the `connect.sid` cookie. The Record ID for CodiMD document is the part of the url that follows the main domain address (e.g. in `https://codimd.web.cern.ch/KabpdG3TTHKOsig2lq8tnw#` the Record ID is `KabpdG3TTHKOsig2lq8tnw`)

### SSO Comp

TODO

## References

- https://flvc.libguides.com/c.php?g=997766&p=8188550
