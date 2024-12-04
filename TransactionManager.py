from Transaction import Transaction
from DataManager import DataManager

class TransactionManager:
    def __init__(self):
        self.sites = {i: DataManager(i) for i in range(1, 11)}  # 10 sites, indexed 1 to 10
        self.transactions = {}  # Active transactions: transaction_id -> Transaction object
        self.site_status = {i: "up" for i in range(1, 11)}  # Site status: "up"/"down"
        self.failure_history = {i: [] for i in range(1, 11)}  # Failure history: site_id -> list of failure/recovery events
        self.waiting_read_queue = []  # Queue for waiting reads: list of (transaction_id, variable, site_id)

        # Initialize data variables
        self.initialize_data()

    def initialize_data(self):
        """Initialize the 20 variables across the 10 sites based on their index."""
        for i in range(1, 21):  # Variables x1 to x20
            initial_value = 10 * i
            if i % 2 == 0:  # Even-indexed variables
                for site_id in self.sites:
                    self.sites[site_id].write(f"x{i}", initial_value, 0)
            else:  # Odd-indexed variables
                site_id = 1 + (i % 10)
                self.sites[site_id].write(f"x{i}", initial_value, 0)

    def start_transaction(self, transaction_id, timestamp, is_read_only=False):
        """Begin a new transaction."""
        print(f"Starting {'read-only ' if is_read_only else ''}transaction T{transaction_id} at timestamp {timestamp}.")
        self.transactions[transaction_id] = Transaction(transaction_id, timestamp, is_read_only)

    def read_intention(self, transaction_id, variable):
        """
        Add a read intention to the transaction's instruction queue and attempt the read immediately.
        Calls the DataManager's read method to retrieve the value.
        Aborts the transaction if no valid site can provide the value.
        """
        if transaction_id not in self.transactions:
            raise Exception(f"Transaction T{transaction_id} does not exist.")

        transaction = self.transactions[transaction_id]

        # Add the variable to the transaction's read set
        transaction.add_read(variable)

        # Determine the sites where the variable is stored
        sites_to_read = []
        variable_index = int(variable[1:])  # Extract the numeric part of the variable name

        if variable_index % 2 == 0:
            # Even-indexed variables (replicated): available on all sites
            sites_to_read = [site_id for site_id, site in self.sites.items()]
        else:
            # Odd-indexed variables (non-replicated): stored on a single site
            site_id = 1 + (variable_index % 10)
            
            sites_to_read = [site_id]

        # Attempt to read from the available sites
        for site_id in sites_to_read:
            if self.site_status[site_id] == "up":
                try:
                    # Calculate the last commit time for the variable
                    last_commit_time = None
                    for value, commit_time in reversed(self.sites[site_id].version_history[variable]):
                        if commit_time <= transaction.start_time:
                            last_commit_time = commit_time
                            break

                    # Check failure history before attempting the read
                    if last_commit_time is not None:
                        for failure_time, status in self.failure_history[site_id]:
                            if last_commit_time < failure_time < transaction.start_time:
                                #print(f"Skipping Site {site_id} for {variable}: Last commit time {last_commit_time} is invalid "
                                #    f"due to failure at {failure_time}.")
                                raise Exception("Site not functional during required period.")
                    
                    # Attempt to read from the site
                    value = self.sites[site_id].read(variable, transaction.start_time)
                    print(f"Transaction T{transaction_id} read {variable}:{value} from Site {site_id}.")
                    return value  # Return the first successful read
                except Exception as e:
                    pass
                    #print(f"Transaction T{transaction_id} failed to read {variable} from Site {site_id}: {e}")
            else:
                try:
                    # Calculate the last commit time for the variable
                    last_commit_time = None
                    for value, commit_time in reversed(self.sites[site_id].version_history[variable]):
                        if commit_time <= transaction.start_time:
                            last_commit_time = commit_time
                            break

                    # Check failure history before attempting the read
                    if last_commit_time is not None:
                        for failure_time, status in self.failure_history[site_id]:
                            if last_commit_time < failure_time < transaction.start_time:
                                #print(f"Skipping Site {site_id} for {variable}: Last commit time {last_commit_time} is invalid "
                                #    f"due to failure at {failure_time}.")
                                raise Exception("Site not functional during required period.")
                    
                    self.waiting_read_queue.append([transaction_id, variable_index,site_id])
                    return 
                except Exception as e:
                    pass

        '''
        for site_id, site in self.sites.items():
            if self.site_status[site_id] == "down" and variable in site.version_history:
                last_commit_time = None
                check = None
                for value, commit_time in reversed(self.sites[site_id].version_history[variable]):
                    if commit_time <= transaction.start_time:
                        last_commit_time = commit_time
                        check = value
                        break

                for failure_time, status in self.failure_history[site_id]:
                    if status == "down" and last_commit_time < failure_time < transaction.start_time:
                        continue
                    else:    
                        value = check
                        #print(f"Transaction T{transaction_id} read {variable}:{value} from failed Site {site_id} "
                        #    f"using functional period validation.")
                        return value
        '''
# If no valid site can provide the value, abort the transaction
        print(f"Transaction T{transaction_id} aborted: No valid site could provide the value for {variable}.")
        transaction.status = "aborted"
        return None


    def write_intention(self, transaction_id, variable, value,timestamp):
        """
        Add a write intention to the transaction's instruction queue.
        The actual write will be handled during commit.
        """
        if transaction_id not in self.transactions:
            raise Exception(f"Transaction T{transaction_id} does not exist.")
        
        transaction = self.transactions[transaction_id]
        transaction.add_write(variable, value,timestamp)
        #print(f"Transaction T{transaction_id} added write intention for {variable} = {value}.")

    def commit(self, transaction_id,time):
        """
        Commit a transaction after validation.
        Implements:
        1. First Committer Wins rule.
        2. Abort if any write timestamp precedes the failure timestamp of a site.
        """
        if transaction_id not in self.transactions:
            raise Exception(f"Transaction T{transaction_id} does not exist.")

        transaction = self.transactions[transaction_id]

        # Check for First Committer Wins violation and failure timestamp validation
        for variable, (value, write_timestamp) in transaction.write_set.items():
            for site_id, site in self.sites.items():
                if self.site_status[site_id] == "up" and (variable in site.variables or variable.startswith("x")):
                    # First Committer Wins Check
                    if variable in site.version_history:
                        last_commit_time = site.version_history[variable][-1][1]
                        if last_commit_time > transaction.start_time:
                            print(f"Transaction T{transaction_id} aborted: {variable} was committed at {last_commit_time}, "
                                f"after transaction start time {transaction.start_time}.")
                            transaction.status = "aborted"
                            return
                    
                # Failure Timestamp Validation
                for failure_timestamp, status in self.failure_history[site_id]:
                    if status == "down" and write_timestamp < failure_timestamp:
                        print(f"Transaction T{transaction_id} aborted: Write timestamp {write_timestamp} for {variable} "
                            f"precedes failure timestamp {failure_timestamp} on Site {site_id}.")
                        transaction.status = "aborted"
                        return

        # Process all read intentions (optional logging for debug)
        #for variable in transaction.read_set:
        #    print(f"Transaction T{transaction_id} reads {variable} during commit.")

        # Process all write intentions
        # Initialize a set to track written sites

        for variable, (value, write_timestamp) in transaction.write_set.items():
            # Distribute writes to the appropriate sites
            written_sites = set()
            variable_index = int(variable[1:])  # Extract the numeric part of the variable name

            if variable_index % 2 == 0:
                # Even-indexed variables: Write to all sites that are up
                for site_id, site in self.sites.items():
                    if self.site_status[site_id] == "up" and variable in site.variables:
                        # Retrieve the last recovery timestamp
                        last_recovery_time = max(
                            (timestamp for timestamp, status in self.failure_history[site_id] if status == "up"),
                            default=None
                        )

                        # Check if the write timestamp is valid
                        if last_recovery_time is not None and write_timestamp < last_recovery_time:
                            continue

                        # Perform the write and track the site
                        site.write(variable, value, write_timestamp)
                        written_sites.add(site_id)
            else:
                # Odd-indexed variables: Write to a single designated site
                site_id = 1 + (variable_index % 10)
                if self.site_status[site_id] == "up" and variable in self.sites[site_id].variables:
                    # Retrieve the last recovery timestamp
                    last_recovery_time = max(
                        (timestamp for timestamp, status in self.failure_history[site_id] if status == "up"),
                        default=None
                    )

                    # Check if the write timestamp is valid
                    if last_recovery_time is not None and write_timestamp < last_recovery_time:
                        continue

                    # Perform the write and track the site
                    self.sites[site_id].write(variable, value, write_timestamp)
                    written_sites.add(site_id)

            # Print the sites written to in a single line
            if written_sites:
                written_sites_list = sorted(written_sites)  # Sort for consistent output
                print(f"Transaction T{transaction_id} wrote {variable} to sites: {', '.join(map(str, written_sites_list))}")



        # Mark the transaction as committed
        transaction.status = "committed"
        print(f"Transaction T{transaction_id} has been committed.")



    def update_site_status(self, site_id, status, timestamp):
        """
        Update the status of a site (up/down).
        Records the failure or recovery event with a timestamp.
        """
        if site_id not in self.sites:
            raise Exception(f"Site {site_id} does not exist.")
        
        if status == "down":
            if self.site_status[site_id] != "down":
                self.sites[site_id].fail()
                self.failure_history[site_id].append((timestamp, "down"))
                self.site_status[site_id] = status
        elif status == "up":
            if self.site_status[site_id] != "up":
                # Recover the site
                self.sites[site_id].recover()
                self.failure_history[site_id].append((timestamp, "up"))
                self.site_status[site_id] = status
                print(f"Site {site_id} has been recovered.")

                # Process the waiting read queue
                for entry in list(self.waiting_read_queue):  # Use a copy to allow modification during iteration
                    transaction_id, variable_index, waiting_site_id = entry
                    if waiting_site_id == site_id:  # Check if the recovered site matches the waiting read site
                        variable = f"x{variable_index}"  # Convert the variable index back to its name
                        try:
                            value = self.sites[site_id].read(variable, self.transactions[transaction_id].start_time)
                            print(f"Transaction T{transaction_id} read {variable}:{value} from recovered Site {site_id}.")
                            # Remove the entry from the queue since it has been processed
                            self.waiting_read_queue.remove(entry)
                        except Exception as e:
                            print(f"Transaction T{transaction_id} failed to read {variable} from recovered Site {site_id}: {e}")

                
                
        self.site_status[site_id] = status
        #print(f"Site {site_id} status updated to {status} at timestamp {timestamp}.")

    def get_failure_history(self, site_id):
        """
        Retrieve the failure history of a specific site.
        """
        if site_id not in self.failure_history:
            raise Exception(f"Site {site_id} does not exist.")
        return self.failure_history[site_id]

    def querystate(self):
        """Print the current state of the system for debugging."""
        print("\n--- Dump State ---")
        for site_id, dm in self.sites.items():
            # Sort variables by the numeric part of their names
            sorted_variables = sorted(dm.variables.items(), key=lambda item: int(item[0][1:]))

            # Format variable-value pairs as a string
            site_data = ", ".join(f"{var}: {val}" for var, val in sorted_variables)

            # Check the site status and include it in the output
            if self.site_status[site_id] == "down":
                print(f"site {site_id} (down) – {site_data}")
            else:
                print(f"site {site_id} – {site_data}")
        print("--------------------")


