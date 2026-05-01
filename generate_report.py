def get_top_tokens(file_name, only_words=False):
    tokens_list = []
    with open(file_name, "r") as file:
        for line in file.readlines():
            token, count = line.strip().split(":")
            tokens_list.append((token, int(count)))
    tokens_list = sorted(tokens_list, key = lambda tup: tup[1], reverse=True)[:50]
    print("---------TOP-50-WORDS---------")
    if only_words:
        for i in tokens_list:
            print(i[0])
    else:
        for i in tokens_list:
            print(f"{i[0]}:{i[1]}")
    print("-------------------")

def count_entries(file_name):
    count = 0
    with open(file_name, "r") as file:
        for line in file.readlines():
            count+=1
    print(f"{file_name} has {count} entries.")

def get_longest_page(file_name):
    print("-------LONGEST-PAGE-------")
    with open(file_name, "r") as file:
        lines = file.readlines()
        print(f"{lines[0].strip()}\nWith a word count of: {lines[1].strip()}")

def main():
    get_top_tokens("token_counts.txt")
    count_entries("unique_urls.txt")
    count_entries("subdomains.txt")
    get_longest_page("longest_doc.txt")


if __name__ == "__main__":
    main()