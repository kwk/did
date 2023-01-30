# coding: utf-8
"""
Phabricator stats for authored and reviewed differentials, aka reviews.

Config example::

    [phabricator]
    type = phabricator
    url = https://reviews.llvm.org/api/
    token = <authentication-token>
    token_file = <file-with-authentication-token>
    login = <username1>,<username2>

The authentication token is *not* optional. Go to
https://reviews.llvm.org/settings/user/<username>/page/apitokens/ and
get yourself a "Conduit API token". The token and the actual users for
which we query stats are decoupled, allowing you to specify more than
one username.

We use this endpoint for the most part
https://reviews.llvm.org/conduit/method/differential.revision.search/.

"""

import datetime
from enum import Enum
from typing import Any, Dict, List

import requests

from did.base import Config, ConfigError, ReportError, get_token
from did.stats import Stats, StatsGroup
from did.utils import listed, log, pretty

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  Investigator
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class Phabricator:
    """ Phabricator Investigator """

    # Maximum number of entries to be fetched per page
    # See "Paging and Limits" section here for example:
    # https://reviews.llvm.org/conduit/method/differential.revision.search/
    MAX_PAGE_SIZE = 100

    def __init__(self, url, token, logins):
        """ Initialize url and headers """
        self.url = url.rstrip("/")
        self.token = token
        self.logins = logins
        self._login_phids = []

    @property
    def login_phids(self) -> List[str]:
        """
        Returns the PHIDs for the login usernames.

        Returns:
            List[str]: The phabricator PHIDs for the login names

        TODO(kwk): The return type could be just `list[str]` but the
                   Copr epel builder currently only has python 3.6 where
                   this is not possible.
        """
        if self._login_phids is None or self._login_phids == []:
            log.debug("Resolving logins to Phabricator PHIDs: %s", self.logins)
            # Resolve logins to phids for users
            # see https://reviews.llvm.org/conduit/method/user.search/
            url = self.url + "/user.search"
            data_dict = {}
            for idx, login in enumerate(self.logins):
                data_dict[f'constraints[usernames][{idx}]'] = login

            results = self._get_all_pages(url, data_dict)
            self._login_phids = [user["phid"] for user in results]
        return self._login_phids

    def search_diffs(self,
                     verbose: bool = False,
                     since: datetime.date = None,
                     until: datetime.date = None,
                     author_phids: List[str] = None,
                     reviewer_phids: List[str] = None) -> List["Differential"]:
        """ Find Phabricator Differentials """
        url = self.url + "/differential.revision.search"
        result = []
        data_dict = {}
        if author_phids is not None:
            for idx, login in enumerate(author_phids):
                data_dict[f'constraints[authorPHIDs][{idx}]'] = login
        if reviewer_phids is not None:
            for idx, login in enumerate(reviewer_phids):
                data_dict[f'constraints[authorPHIDs][{idx}]'] = login
        if since is not None:
            data_dict['constraints[createdStart]'] = since.strftime("%s")
        if until is not None:
            data_dict['constraints[createdEnd]'] = until.strftime("%s")
        for diff in self._get_all_pages(url, data_dict):
            result.append(Differential(diff, verbose=verbose))
        log.data(pretty(result))
        return result

    def search_transactions(self,
                            diff_phid: str,
                            author_phids: List[str] = None) -> List["TransactionEvent"]:
        """
        Returns all the transaction events for a given differential
        object. If given you can search for events by certain authors
        authors.
        """
        url = self.url + "/transaction.search"
        data_dict = {}
        data_dict["objectIdentifier"] = diff_phid
        if author_phids is not None:
            for idx, login in enumerate(author_phids):
                data_dict[f'constraints[authorPHIDs][{idx}]'] = login
        events = self._get_all_pages(url, data_dict)
        return [TransactionEvent(event) for event in events]

    def _get_all_pages(self, url: str, data_dict: Dict[str, Any]):
        """
        Gets all pages of a Phabricator Conduit API request; given that
        the API is pageable.
        """
        if data_dict is None:
            data_dict = {}
        data_dict['after'] = None
        results = []
        while True:
            res = self._get_page(url, data_dict)
            if "result" not in res:
                raise ReportError("Mising key Phabricator dict: result")
            results.extend(res["result"]["data"])
            # Define offset of next differentials to fetch
            if "cursor" in res:
                data_dict['after'] = res["cursor"]["after"]
                if data_dict['after'] is None:
                    break
            else:
                break
        log.debug("Results: %s fetched", listed(len(results), "item"))
        return results

    def _get_page(self, url: str, data_dict: Dict[str, Any]):
        """
        Gets a single page of a Phabricator Conduit API request
        """
        if data_dict is None:
            data_dict = {}
        if "limit" not in data_dict:
            data_dict['limit'] = Phabricator.MAX_PAGE_SIZE
        if "api.token" not in data_dict:
            data_dict['api.token'] = self.token
        try:
            response = requests.post(url, data=data_dict)
            log.debug("Response headers: %s", response.headers)
        except requests.exceptions.RequestException as error:
            log.debug(error)
            raise ReportError(
                f"Phabricator search on {url} failed") from error

        if response.status_code != 200:
            log.debug("Phabricator status code: {response.status.code}")
            raise RuntimeError(
                "Phabricator request exited with status code "
                f"{response.status_code} rather than 200.")

        try:
            decoded = response.json()
            # Handle API errors
            if decoded["error_info"] is not None:
                raise RuntimeError(
                    f"Phabricator error encountered: {decoded['error_info']}")
        except requests.exceptions.JSONDecodeError as error:
            log.debug(error)
            raise ReportError(
                "Phabricator failed to parse JSON response.") from error
        return decoded


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  Differential
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class Differential:  # pylint: disable=too-few-public-methods
    """
    Phabricator Differential

    Here's an example::

        {
         "id": 134852,
         "type": "DREV",
         "phid": "PHID-DREV-6vyvvzlqatx6a4oveqkw",
         "fields":
         {
          "title": "[clang-format][NFC] Clean up class HeaderIncludes",
          "uri": "https://reviews.llvm.org/D134852",
          "authorPHID": "PHID-USER-vou2cb5rty2zlopptj5z",
          "status":
          {
            "value": "published",
            "name": "Closed",
            "closed": true,
            "color.ansi": "cyan"
          },
          "repositoryPHID": "PHID-REPO-f4scjekhnkmh7qilxlcy",
          "diffPHID": "PHID-DIFF-6qic23rkxpwvkp6g4wdg",
          "summary": "",
          "testPlan": "",
          "isDraft": false,
          "holdAsDraft": false,
          "dateCreated": 1664433452,
          "dateModified": 1665032091,
          "policy":
          {
           "view": "public",
           "edit": "users"
          }
         },
         "attachments": {}
        }

    """

    def __init__(self, data, verbose: bool = False):
        self._data = data
        if verbose:
            self.str = f'{data["fields"]["uri"]} {data["fields"]["title"]}'
        else:
            self.str = f'D{data["id"]} {data["fields"]["title"]}'

    @property
    def phid(self) -> str:
        """
        Returns the Phabricator ID for the differential as a string
        """
        return self._data["phid"]

    def __str__(self):
        """ String representation """
        return self.str

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  TransactionEvent
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class EventType(Enum):
    """
    EventType defines what type of transaction events we support.
    """
    COMMENT = "comment"
    INLINE = "inline"
    CREATE = "create"
    CLOSE = "close"
    UPDATE = "update"
    SUMMARY = "summary"
    TITLE = "title"
    PROJECTS = "projects"
    REQUEST_REVIEW = "request-review"
    REVIEWERS = "reviewers"
    SUBSCRIBERS = "subscribers"
    STATUS = "status"
    UNDEFINED = ""

    def __str__(self):
        """ String representation """
        return self.value


class TransactionEvent:  # pylint: disable=too-few-public-methods
    """
    Phabricator Transaction event.

    See https://reviews.llvm.org/conduit/method/transaction.search/.

    Here're examples::

        {
            "id": 4077171,
            "phid": "PHID-XACT-DREV-7grkcftntvxf24c",
            "type": "create",
            "authorPHID": "PHID-USER-m46saogacat2jslbykue",
            "objectPHID": "PHID-DREV-ypgxje4hhhdefuy4d6sz",
            "dateCreated": 1674573526,
            "dateModified": 1674573526,
            "groupID": "dr3e2g6tx6ztr6zivk343kytk7uk7yng",
            "comments": [],
            "fields": {}
        }

        {
            "id": 4077175,
            "phid": "PHID-XACT-DREV-zconyio2dw2y7ne",
            "type": "reviewers",
            "authorPHID": "PHID-USER-m46saogacat2jslbykue",
            "objectPHID": "PHID-DREV-ypgxje4hhhdefuy4d6sz",
            "dateCreated": 1674573526,
            "dateModified": 1674573526,
            "groupID": "dr3e2g6tx6ztr6zivk343kytk7uk7yng",
            "comments": [],
            "fields": {
                "operations": [
                {
                    "operation": "add",
                    "phid": "PHID-USER-aigeqxvzdke5r36hodix",
                    "oldStatus": null,
                    "newStatus": "added",
                    "isBlocking": false
                },
                {
                    "operation": "add",
                    "phid": "PHID-USER-7rdtwvftotyrjl5bf7gy",
                    "oldStatus": null,
                    "newStatus": "added",
                    "isBlocking": false
                },
                {
                    "operation": "add",
                    "phid": "PHID-USER-icssaf6rtj6ahq4lchay",
                    "oldStatus": null,
                    "newStatus": "added",
                    "isBlocking": false
                }
                ]
            }
        },

    """

    def __init__(self, data):
        self._data = data
        self._type = self._data["type"]
        self._author_phid = self._data["authorPHID"]

    def is_in_date_range(self, since: datetime.date = None,
                         until: datetime.date = None) -> bool:
        """
        Returns true if the event happend in the given timestamp range,
        including the boundaries.
        """
        date_modified = datetime.date.fromtimestamp(self._data["dateModified"])
        if since is not None:
            if not date_modified >= since:
                return False
        if until is not None:
            if not date_modified <= until:
                return False
        return True

    def is_type(self, typ: EventType) -> bool:
        """
        Returns true if the transaction refers to an event of the given
        type.
        """
        if typ == EventType.UNDEFINED:
            if self._type is None or self._type == "":
                return True
            return False
        return self._type == str(typ)

    @property
    def event_type(self) -> EventType:
        """ Returns the type of event """
        return self._type

    @property
    def author_phid(self) -> str:
        """ Returns the author's PHID """
        return self._author_phid

    def __str__(self):
        """ String representation """
        return f"{self.author_phid} - {self.event_type} - {self._data['dateModified']}"

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  Stats
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class DifferentialsCommented(Stats):
    """ Differentials commented """

    def fetch(self):
        log.info("Searching for differentials commented by '%s'.", self.user)

        diffs = self.parent.phabricator.search_diffs(
            verbose=self.options.verbose,
            since=self.options.since.date,
            until=self.options.until.date,
            author_phids=self.parent.phabricator.login_phids)
        # Filter out those diffs where there's a comment transaction
        commented_diffs = []
        closed_diffs = []
        for diff in diffs:
            events = self.parent.phabricator.search_transactions(
                diff_phid=diff.phid, author_phids=self.parent.phabricator.login_phids)
            for event in events:
                if not event.is_in_date_range(
                        self.options.since.date,
                        self.options.until.date):
                    print("out of date")
                    continue
                if event.is_type(EventType.COMMENT) or event.is_type(EventType.INLINE):
                    commented_diffs.append(diff)
                elif event.is_type(EventType.CLOSE):
                    closed_diffs.append(diff)
        self.stats = commented_diffs


class DifferentialsCreated(Stats):
    """ Differentials authored """

    def fetch(self):
        log.info("Searching for differentials created by '%s'.", self.user)
        data_dict = {}
        for idx, login in enumerate(self.parent.phabricator.login_phids):
            data_dict[f'constraints[authorPHIDs][{idx}]'] = login
        data_dict['constraints[createdStart]'] = self.options.since.date.strftime("%s")
        data_dict['constraints[createdEnd]'] = self.options.until.date.strftime("%s")
        self.stats = self.parent.phabricator.search_diffs(
            data_dict=data_dict,
            verbose=self.options.verbose)


class DifferentialsReviewed(Stats):
    """ Differentials reviewed """

    def fetch(self):
        log.info("Searching for differentials reviewed by '%s'", self.user)
        data_dict = {}
        for idx, login in enumerate(self.parent.phabricator.login_phids):
            data_dict[f'constraints[reviewerPHIDs][{idx}]'] = login
        data_dict['constraints[modifiedStart]'] = self.options.since.date.strftime("%s")
        data_dict['constraints[modifiedEnd]'] = self.options.until.date.strftime("%s")
        self.stats = self.parent.phabricator.search_diffs(
            data_dict=data_dict,
            verbose=self.options.verbose)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  Stats Group
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class PhabricatorStats(StatsGroup):
    """ Phabricator work """

    # Default order
    order = 360

    def __init__(self, option, name=None, parent=None, user=None):
        StatsGroup.__init__(self, option, name, parent, user)
        config = dict(Config().section(option))
        # Check server url
        if "url" not in config:
            raise ConfigError(
                f"No phabricator url set in the [{option}] section")
        self.url = config["url"]
        # Check authorization token.
        self.token = get_token(config)
        if self.token is None:
            raise ConfigError(
                f"No token or token_file set in the [{option}] section")
        if "login" not in config:
            raise ConfigError(f"No login set in the [{option}] section")
        self.logins = [
            login.strip() for login in str(
                config["login"]).split(",")]
        if self.logins == []:
            raise ConfigError(f"Empty login found in [{option}] setion")
        self.phabricator = Phabricator(self.url, self.token, self.logins)
        # Create the list of stats
        self.stats = [
            DifferentialsCommented(
                option=option + "-differentials-commented", parent=self,
                name=f"Differentials commented on {option}"),
            DifferentialsCreated(
                option=option + "-differentials-created", parent=self,
                name=f"Differentials created on {option}"),
            DifferentialsReviewed(
                option=option + "-differentials-reviewed", parent=self,
                name=f"Differentials participated on {option}"),
            ]
