# DataHarbor

Project Highlight:

The overarching goal of this project is to develop a comprehensive database management system that encompasses multiple components. Firstly, the implementation involves creating a simple storage manager responsible for reading blocks from a file on disk into memory and writing blocks from memory to a file on disk. This storage manager is equipped with methods for file creation, opening, and closing. Furthermore, it is designed to track crucial information about open files, including the total number of pages, current page position, file name, and file pointer.

Building upon the storage manager, the project extends its scope to include a buffer manager capable of efficiently handling a fixed number of memory pages. This buffer manager is interconnected with the storage manager and accommodates multiple open buffer pools simultaneously. Additionally, the buffer manager incorporates two replacement strategies, namely First-In-First-Out (FIFO) and Least Recently Used (LRU), ensuring flexibility in managing data in memory.

To complete the database management system, the project also focuses on implementing a B+ tree index. This component enhances the overall functionality by providing an efficient structure for indexing and organizing the data. Through these combined efforts, the project aims to create a robust and versatile system capable of managing storage, buffering, and indexing seamlessly and effectively.


Instructions to Run the Code:

Run the following commands on a Linux System. 

In the terminal go to the directory and run the following commands.

$ make
$ ./test_assign4_1
$ ./test_assign4_2
$ ./test_expr

$ make clean         //To clean/delete all the object files.

Tech Stack:

C Language

Tools:

Oracle VM VirtualBox 
