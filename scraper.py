import re
from urllib.parse import urlparse
from bs4 import BeautifulSoup

stop_words = ["a",
"about",
"above",
"after",
"again",
"against",
"all",
"am",
"an",
"and",
"any",
"are",
"aren't",
"as",
"at",
"be",
"because",
"been",
"before",
"being",
"below",
"between",
"both",
"but",
"by",
"can't",
"cannot",
"could",
"couldn't",
"did",
"didn't",
"do",
"does",
"doesn't",
"doing",
"don't",
"down",
"during",
"each",
"few",
"for",
"from",
"further",
"had",
"hadn't",
"has",
"hasn't",
"have",
"haven't",
"having",
"he",
"he'd",
"he'll",
"he's",
"her",
"here",
"here's",
"hers",
"herself",
"him",
"himself",
"his",
"how",
"how's",
"i",
"i'd",
"i'll",
"i'm",
"i've",
"if",
"in",
"into",
"is",
"isn't",
"it",
"it's",
"its",
"itself",
"let's",
"me",
"more",
"most",
"mustn't",
"my",
"myself",
"no",
"nor",
"not",
"of",
"off",
"on",
"once",
"only",
"or",
"other",
"ought",
"our",
"ours",
"ourselves",
"out",
"over",
"own",
"same",
"shan't",
"she",
"she'd",
"she'll",
"she's",
"should",
"shouldn't",
"so",
"some",
"such",
"than",
"that",
"that's",
"the",
"their",
"theirs",
"them",
"themselves",
"then",
"there",
"there's",
"these",
"they",
"they'd",
"they'll",
"they're",
"they've",
"this",
"those",
"through",
"to",
"too",
"under",
"until",
"up",
"very",
"was",
"wasn't",
"we",
"we'd",
"we'll",
"we're",
"we've",
"were",
"weren't",
"what",
"what's",
"when",
"when's",
"where",
"where's",
"which",
"while",
"who",
"who's",
"whom",
"why",
"why's",
"with",
"won't",
"would",
"wouldn't",
"you",
"you'd",
"you'll",
"you're",
"you've",
"your",
"yours",
"yourself",
"yourselves"]

def scraper(url, resp, buffer, top_record, urls_dict, subdomain_dict):
    links = extract_next_links(url, resp, buffer, top_record, urls_dict, subdomain_dict)
    return [link for link in links if is_valid(link)]

def get_domain(url):
    try:
        parsed = urlparse(url)
        return parsed.netloc
    except ValueError:
        print(url)
        return ""

def extract_next_links(url, resp, buffer, top_record, urls_dict, subdomain_dict):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    if resp.status != 200:
        print(f"{url}: {resp.error}")
        with open("status_error.txt", "a") as file:
            file.write(f"{resp.status}:{url}\n")
        return []

    soup = BeautifulSoup(resp.raw_response.content, "html.parser")
    urls = soup.find_all(href=True)
    print(urls[:3])
    urls = list([x[6:-1] for x in urls if "ics.uci.edu" in get_domain(x[6:-1]) or "cs.uci.edu" in get_domain(x[6:-1]) or "informatics.uci.edu" in get_domain(x[6:-1]) or "stat.uci.edu" in get_domain(x[6:-1])])
    extract_text(soup, buffer, "token_counts.txt", url, top_record)
    store_url(urls, "unique_urls.txt", urls_dict)
    store_subdomain(urls, "subdomains.txt", subdomain_dict)
    return urls

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        return not (re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz|jp|events/tag/talks/list|events/tag/talks/month)$", parsed.path.lower())
                    or re.match(r".*ical=", parsed.query.lower())
                    or re.match(r".*(/wp-json|events/.*/[0-9]+-[0-9]+(-[0-9]+)*)", parsed.path.lower())
                    or re.match(r".*(isg.ics.uci.edu/)(\?p=[0-9]+|events/.*/)+", url))

    except TypeError:
        print ("TypeError for ", parsed)
        raise

def extract_text(soup:BeautifulSoup, buffer:dict, file_name:str, link:str, top_record):
    text_content = soup.get_text(" ").strip()
    tokens = re.findall(r"[a-zA-Z0-9]+['-]*[a-zA-z0-9]+", text_content.lower())
    store_longest_doc(link, len(tokens), top_record)
    for token in tokens:
        if token in stop_words:
            continue
        buffer[token] = buffer.get(token, 0) + 1
    with open(file_name, "w") as file:
        for i in buffer.keys():
            file.write(f"{i}:{buffer[i]}\n")

def initialize_buffer(file_name):
    tokens_dict = {}
    with open(file_name, "r") as file:
        for line in file.readlines():
            token, count = line.strip().split(":")
            tokens_dict[token] = int(count)
    return tokens_dict

def initialize_url_buffer(file_name):
    url_dict = {}
    with open(file_name, "r") as file:
        for line in file.readlines():
            url_dict[line.strip()] = True
    return url_dict

def store_url(link_list, file_name, link_buffer:dict):
    for url_link in link_list:
        if not is_valid(url_link):
            continue
        parsed_link = urlparse(url_link)
        truncated_link = f"{parsed_link.scheme}://{parsed_link.hostname}{parsed_link.path}"
        if link_buffer.get(truncated_link):
            continue
        else:
            link_buffer[truncated_link] = True
    with open(file_name, "w") as file:
        for key in link_buffer.keys():
            file.write(f"{key}\n")


def initialize_subdomain_buffer(file_name):
    subdomain_dict = {}
    with open(file_name, "r") as file:
        for line in file.readlines():
            subdomain_dict[line.strip()] = True
    return subdomain_dict

def store_subdomain(link_list, file_name, link_buffer):
    for url_link in link_list:
        subdomain = get_domain(url_link)
        if link_buffer.get(subdomain):
            continue
        else:
            link_buffer[subdomain] = True
    with open(file_name, "w") as file:
        for key in link_buffer.keys():
            file.write(f"{key}\n")

def initialize_longest_doc(file_name):
    with open(file_name, "r") as file:
        lines = file.readlines()
        url = "NONE FOUND"
        count = 0
        if len(lines) == 2:
            url = lines[0]
            count = int(lines[1])
        return [url, count]

def store_longest_doc(url, length, top_record):
    if length > top_record[1]:
        top_record[0] = url
        top_record[1] = length
        with open("longest_doc.txt", "w") as file:
            file.write(f"{url}\n{length}")