#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define NUMBER_OF_COORDINATORS 4
#define LAST_COORDINATOR 3
#define FIRST_COORDINATOR 0

// The function returns 1 if the process with the desired rank is one of the
// 4 coordinators, and 0 otherwise.
int rank_is_coordinator(int rank) {
    if (rank == 0 || rank == 1 || rank == 2 || rank == 3){
        return 1;
    }

    return 0;
}

int *allocate_array(int size) {
    int *v = malloc(sizeof(int) * size);
    if (v == NULL) {
        printf("There was a problem at allocating the memory.\n");
        MPI_Finalize();
        exit(1);
    }

    return v;
}

// The function receives the rank of a coordinator and returns the workers
// that are under him. In elems we will have the number of said workers.
int *read_workers_from_file(int rank, int *elems) {
    char buffer[100];
    char name_of_file[13] = "cluster0.txt";
    int number_of_elems_in_file;
    int *workers;

    name_of_file[7] += rank;
    FILE* file = fopen(name_of_file, "r");
    fgets(buffer, 100, file);
    
    // We erase the \n character from the end of the buffer
    buffer[strlen(buffer) - 1] = '\0';
    number_of_elems_in_file = atoi(buffer);

    workers = allocate_array(number_of_elems_in_file);

    for (int i = 0; i < number_of_elems_in_file; i++) {
        fgets(buffer, 100, file);
        buffer[strlen(buffer) - 1] = '\0';
        workers[i] = atoi(buffer);
    }

    *elems = number_of_elems_in_file;

    return workers;
}

// The function assures that every coordinator knows the topology and assures
// the sending of the messages with the topology to each worker. This
// function works only if there are no broken links.
void find_topology_coordinator(int rank, int* topology[NUMBER_OF_COORDINATORS],
                    int number_of_workers_per_cluster[NUMBER_OF_COORDINATORS]) {
    // We firstly send the number of workers in the current cluster, then we send the worker's IDs
    if (rank == 3) {
        MPI_Send(&number_of_workers_per_cluster[3] , 1, MPI_INT, 0, 1, MPI_COMM_WORLD);
        printf("M(%d,%d)\n", rank, FIRST_COORDINATOR); // We log the messages

        MPI_Send(topology[3], number_of_workers_per_cluster[3], MPI_INT , 0, 1, MPI_COMM_WORLD);
        printf("M(%d,%d)\n", rank, FIRST_COORDINATOR);
    }
    else {
        MPI_Send(&number_of_workers_per_cluster[rank], 1, MPI_INT , rank + 1, 1, MPI_COMM_WORLD);
        printf("M(%d,%d)\n", rank, rank + 1);

        MPI_Send(topology[rank], number_of_workers_per_cluster[rank], MPI_INT, rank + 1, 1, MPI_COMM_WORLD);
        printf("M(%d,%d)\n", rank, rank + 1);
    }

    // We then receive the topology step by step from the previous coordinator.
    // We stop after 3 receives because there are 4 coordinators, and each one of
    // them knows about itself. After every receive, we send the information further.
    for (int i = 0; i < 3; i++) {
        MPI_Status status;
        if (rank == 0) {
            MPI_Recv(&number_of_workers_per_cluster[3 - i], 1, MPI_INT, 3, 1, MPI_COMM_WORLD, &status);
            
            topology[3 - i] = allocate_array(number_of_workers_per_cluster[3 - i]);

            MPI_Recv(topology[3 - i], number_of_workers_per_cluster[3 - i], MPI_INT, 3, 1, MPI_COMM_WORLD, &status);

            // We don't forward the last received because we would send
            // to the coordinator the topology that he already has.
            if (i != 2) {
                MPI_Send(&number_of_workers_per_cluster[3 - i], 1, MPI_INT , rank + 1, 1, MPI_COMM_WORLD);
                printf("M(%d,%d)\n", 0, rank + 1);

                MPI_Send(topology[3 - i], number_of_workers_per_cluster[3 - i], MPI_INT, rank + 1, 1, MPI_COMM_WORLD);
                printf("M(%d,%d)\n", 0, rank + 1);
            }
        }
        else {
            MPI_Recv(&number_of_workers_per_cluster[(3 + rank - i) % 4], 1, MPI_INT, rank - 1, 1, MPI_COMM_WORLD, &status);
            
            topology[(3 + rank - i) % 4] = allocate_array(number_of_workers_per_cluster[(3 + rank - i) % 4]);

            MPI_Recv(topology[(3 + rank - i) % 4], number_of_workers_per_cluster[(3 + rank - i) % 4], MPI_INT, rank - 1, 1, MPI_COMM_WORLD, &status);

            if (i != 2) {
                int next = (rank == 3) ? 0 : rank + 1;
                MPI_Send(&number_of_workers_per_cluster[(3 + rank - i) % 4], 1, MPI_INT , next, 1, MPI_COMM_WORLD);
                printf("M(%d,%d)\n", rank, next);

                MPI_Send(topology[(3 + rank - i) % 4], number_of_workers_per_cluster[(3 + rank - i) % 4], MPI_INT, next, 1, MPI_COMM_WORLD);
                printf("M(%d,%d)\n", rank, next);
            }
        }
    }
}

void find_topology_coordinator_broken_link(int rank, int number_of_workers_per_cluster[NUMBER_OF_COORDINATORS],
    int* topology[NUMBER_OF_COORDINATORS], int last_rank) {
    MPI_Status status;
    int next_rank = rank - 1;

    if (rank == FIRST_COORDINATOR) next_rank = LAST_COORDINATOR;

    // The process 1 doesn't send anything in the first part, it just receives
    if (rank == FIRST_COORDINATOR || rank > last_rank) {
        MPI_Send(&number_of_workers_per_cluster[rank], 1, MPI_INT, next_rank, 1, MPI_COMM_WORLD);
        printf("M(%d,%d)\n", rank, next_rank);

        MPI_Send(topology[rank], number_of_workers_per_cluster[rank], MPI_INT,
                next_rank, 1, MPI_COMM_WORLD);
        printf("M(%d,%d)\n", rank, next_rank);
    }

    if (rank != FIRST_COORDINATOR && rank >= last_rank) {
        // We receive parts of the topology from our neighbor, and then we
        // forward it, unless we are the last in the "chain"
        for (int i = 0; i < NUMBER_OF_COORDINATORS - rank; i++) {
            MPI_Recv(&number_of_workers_per_cluster[(rank + 1 + i) % NUMBER_OF_COORDINATORS],
            1, MPI_INT, (rank + 1) % NUMBER_OF_COORDINATORS, 1, MPI_COMM_WORLD, &status);

            topology[(rank + 1 + i) % NUMBER_OF_COORDINATORS] = allocate_array
                (number_of_workers_per_cluster[(rank + 1 + i) % NUMBER_OF_COORDINATORS]);

            MPI_Recv(topology[(rank + 1 + i) % NUMBER_OF_COORDINATORS], 
            number_of_workers_per_cluster[(rank + 1 + i) % NUMBER_OF_COORDINATORS],
                MPI_INT, (rank + 1) % NUMBER_OF_COORDINATORS, 1, MPI_COMM_WORLD, &status);

            if (rank > last_rank) {
                MPI_Send(&number_of_workers_per_cluster[(rank + 1 + i) % NUMBER_OF_COORDINATORS],
                1, MPI_INT, next_rank, 1, MPI_COMM_WORLD);
                printf("M(%d,%d)\n", rank, next_rank);

                MPI_Send(topology[(rank + 1 + i) % NUMBER_OF_COORDINATORS],
                number_of_workers_per_cluster[(rank + 1 + i) % NUMBER_OF_COORDINATORS],
                MPI_INT, next_rank, 1, MPI_COMM_WORLD);
                printf("M(%d,%d)\n", rank, next_rank);
            }
        }
    }

    next_rank = (rank + 1) % NUMBER_OF_COORDINATORS;
    if (rank != FIRST_COORDINATOR && rank >= last_rank) {
        MPI_Send(&number_of_workers_per_cluster[rank], 1, MPI_INT, next_rank, 1, MPI_COMM_WORLD);
        printf("M(%d,%d)\n", rank, next_rank);

        MPI_Send(topology[rank], number_of_workers_per_cluster[rank], MPI_INT, next_rank, 1, MPI_COMM_WORLD);
        printf("M(%d,%d)\n", rank, next_rank);
    }

    if (rank > last_rank || rank == FIRST_COORDINATOR) {
        int prev_rank = (rank == FIRST_COORDINATOR) ? 3 : rank - 1;
        int num_of_recv = prev_rank - (last_rank - 1);
        for (int i = 0; i < num_of_recv; i++) {
            MPI_Recv(&number_of_workers_per_cluster[prev_rank - i], 1, MPI_INT, prev_rank, 1, MPI_COMM_WORLD, &status);

            topology[prev_rank - i] = allocate_array(number_of_workers_per_cluster[prev_rank - i]);

            MPI_Recv(topology[prev_rank - i], number_of_workers_per_cluster[prev_rank - i],
            MPI_INT, prev_rank, 1, MPI_COMM_WORLD, &status);

            if (rank != FIRST_COORDINATOR && rank >= last_rank) {
                MPI_Send(&number_of_workers_per_cluster[prev_rank - i], 1, MPI_INT, next_rank, 1, MPI_COMM_WORLD);
                printf("M(%d,%d)\n", rank, next_rank);

                MPI_Send(topology[prev_rank - i], number_of_workers_per_cluster[prev_rank - i],
                MPI_INT, next_rank, 1, MPI_COMM_WORLD);
                printf("M(%d,%d)\n", rank, next_rank);
            }
        }
    }
}

void print_topology(int rank, int* topology[NUMBER_OF_COORDINATORS], 
                    int number_of_workers_per_cluster[NUMBER_OF_COORDINATORS]) {

    printf("%d -> ", rank);
    for (int i = 0; i < 4; i++) {
        if (number_of_workers_per_cluster[i] > 0) {
            printf("%d:", i);

            for (int j = 0; j < number_of_workers_per_cluster[i]; j++) {
                printf("%d",topology[i][j]);
                if (j < number_of_workers_per_cluster[i] - 1) {
                    printf(",");
                }
            }

            printf(" ");
        }
    }
    printf("\n");
}

// We send a message to the workers, so they know who their coordinator is.
// The content of the message doesn't matter.
void send_hello_message(int rank, int* workers, int number_of_workers) {
    for (int i = 0; i < number_of_workers; i++) {
        int msg = 0;
        MPI_Send(&msg, 1, MPI_INT, workers[i], 1, MPI_COMM_WORLD);
        printf("M(%d,%d)\n", rank, workers[i]);
    }
}

// The function receives the hello message and returns the rank of the process
// that sent it, so we know who is this process' coordinator.
int recv_hello_message() {
    MPI_Status status;
    int x;
    MPI_Recv(&x, 1, MPI_INT, MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, &status);

    return status.MPI_SOURCE;
}

// The function assures that every coordinator sends the topology to each
// of its assigned workers.
void send_topology_to_workers(int rank, int *topology[NUMBER_OF_COORDINATORS],
                    int number_of_workers_per_cluster[NUMBER_OF_COORDINATORS]) {
    for (int i = 0; i < number_of_workers_per_cluster[rank]; i++) {
        for (int j = 0; j < NUMBER_OF_COORDINATORS; j++) {
            MPI_Send(&number_of_workers_per_cluster[j], 1, MPI_INT,
             topology[rank][i], 1, MPI_COMM_WORLD);
             printf("M(%d,%d)\n", rank, topology[rank][i]);

            MPI_Send(topology[j], number_of_workers_per_cluster[j], MPI_INT,
            topology[rank][i], 1, MPI_COMM_WORLD);
            printf("M(%d,%d)\n", rank, topology[rank][i]);
        }
    }
}

// The function receives the topology from the coordinator.
void recv_topology_from_coordinator(int *topology[NUMBER_OF_COORDINATORS],
        int number_of_workers_per_cluster[NUMBER_OF_COORDINATORS], int coord) {

    MPI_Status status;

    for (int i = 0; i < NUMBER_OF_COORDINATORS; i++) {
        MPI_Recv(&number_of_workers_per_cluster[i], 1, MPI_INT, coord, 1,
                 MPI_COMM_WORLD , &status);

        topology[i] = allocate_array(number_of_workers_per_cluster[i]);

        MPI_Recv(topology[i], number_of_workers_per_cluster[i], MPI_INT, coord, 1,
         MPI_COMM_WORLD, &status);
    }
}

// The function returns the number of workers that don't have work assigned yet,
// based on the fact that the workers of previous coordinator were already taken
// care of.
int number_of_workers_remaining(int rank, 
    int number_of_workers_per_cluster[NUMBER_OF_COORDINATORS]) {

    int number = 0;
    for (int i = rank; i < NUMBER_OF_COORDINATORS; i++) {
        number += number_of_workers_per_cluster[i];
    }

    return number;
}

// The function will be called by a worker. The workers receive their part of
// work and then send back the results.
void make_work(int rank, int coordinator_rank) {
    int number_of_elements;
    int *v;
    MPI_Status status;
    MPI_Recv(&number_of_elements, 1, MPI_INT, coordinator_rank, 1, MPI_COMM_WORLD, &status);

    v = allocate_array(number_of_elements);
    MPI_Recv(v, number_of_elements, MPI_INT, coordinator_rank, 1, MPI_COMM_WORLD, &status);

    for (int i = 0; i < number_of_elements; i++) {
        v[i] = v[i] * 5;
    }

    MPI_Send(&number_of_elements, 1, MPI_INT, coordinator_rank, 1, MPI_COMM_WORLD);
    printf("M(%d,%d)\n", rank, coordinator_rank);

    MPI_Send(v, number_of_elements, MPI_INT, coordinator_rank, 1, MPI_COMM_WORLD);
    printf("M(%d,%d)\n", rank, coordinator_rank);

    free (v);
}

void send_work_to_next_coordinator(int rank, int elements_left, int number_of_elements, int *v) {
    int next_coord = rank - 1;
    if (rank == FIRST_COORDINATOR) next_coord = LAST_COORDINATOR;

    // We send to the next coordinator the rest of the array
    MPI_Send(&elements_left, 1, MPI_INT, next_coord, 1, MPI_COMM_WORLD);
    printf("M(%d,%d)\n", rank, next_coord);

    MPI_Send((v + number_of_elements - elements_left), elements_left, MPI_INT,
    next_coord, 1, MPI_COMM_WORLD);
    printf("M(%d,%d)\n", rank, next_coord);
}

// The function sends the designated work to every worker, and then we receive
// the results.
void send_and_recv(int rank, int number_of_elements_per_worker, int workers,
    int *topology[NUMBER_OF_COORDINATORS], int *v, int modulo) {

    int index = 0;
    MPI_Status status;

    // We first send the work, and then we receive the result
    for (int i = 0; i < workers; i++) {
        int elements = number_of_elements_per_worker;

        if (modulo > 0) {
            elements++;
            modulo--;
        }

        MPI_Send(&elements, 1, MPI_INT, topology[rank][i],
        1, MPI_COMM_WORLD);
        printf("M(%d,%d)\n", rank, topology[rank][i]);

        MPI_Send((v + index), elements,
            MPI_INT, topology[rank][i], 1, MPI_COMM_WORLD);
        printf("M(%d,%d)\n", rank, topology[rank][i]);

        index += elements;
    }

    index = 0;
    // We receive the results
    for (int i = 0; i < workers; i++) {
        int number;
        MPI_Recv(&number, 1, MPI_INT, topology[rank][i], 1, MPI_COMM_WORLD, &status);
        MPI_Recv((v + index), number, MPI_INT, topology[rank][i], 1, MPI_COMM_WORLD, &status);
        index += number;
    }
}

void print_result(int *v, int number_of_elements) {
    printf ("Rezultat: ");
        for (int i = 0; i < number_of_elements; i++) {
            printf ("%d", v[i]);

            if (i < number_of_elements - 1) {
                printf (" ");
            }
        }
        printf ("\n");
}

// Every coordinator will aggregate its own results and send them to the first
// coordinator. This function ensures that the coordinators between the current
// one and the first one will forward the messages.
void forward_results(int rank, int elements_to_workers, int *v, int number_of_elements, int last_rank) {
    MPI_Status status;
    int next_rank = (rank + 1) % NUMBER_OF_COORDINATORS;

    // We send backwards our work, and then we forward, still backwards,
    // the other coordinator's work
    MPI_Send(&elements_to_workers, 1, MPI_INT, next_rank, 1, MPI_COMM_WORLD);
    printf("M(%d,%d)\n", rank, next_rank);

    MPI_Send(v, elements_to_workers, MPI_INT, next_rank, 1, MPI_COMM_WORLD);
    printf("M(%d,%d)\n", rank, next_rank);
    free (v);

    // 1 will not forward anything, 2 will forward one message, and 3 two.
    int num_of_forwards = rank - 1 - (last_rank - 1);
    for (int i = 0; i < num_of_forwards; i++) {
        MPI_Recv(&number_of_elements, 1, MPI_INT, rank - 1, 1, MPI_COMM_WORLD, &status);
        v = allocate_array(number_of_elements);
        MPI_Recv(v, number_of_elements, MPI_INT, rank - 1, 1, MPI_COMM_WORLD, &status);
        MPI_Send(&number_of_elements, 1, MPI_INT, next_rank, 1, MPI_COMM_WORLD);
        printf("M(%d,%d)\n", rank, next_rank);

        MPI_Send(v, number_of_elements, MPI_INT, next_rank, 1, MPI_COMM_WORLD);
        printf("M(%d,%d)\n", rank, next_rank);
        free (v);
    }
}

// The function assures that a coordinator will distrubute work to the workers
// and aggregate the correct results from them and forward all the messages towards
// the first coordinator.
void coordinate_work(int rank, int number_of_elements, 
    int number_of_workers_per_cluster[NUMBER_OF_COORDINATORS], 
    int *topology[NUMBER_OF_COORDINATORS], int *v, int last_rank) {

    MPI_Status status;

    // The first coordinator starts to send out work, it doesn't receive it
    if (rank != FIRST_COORDINATOR && rank >= last_rank) {
        int prev_rank = (rank + 1) % NUMBER_OF_COORDINATORS;
        MPI_Recv(&number_of_elements, 1, MPI_INT, prev_rank, 1, MPI_COMM_WORLD, &status);
        v = allocate_array(number_of_elements);
        MPI_Recv(v, number_of_elements, MPI_INT, prev_rank, 1, MPI_COMM_WORLD, &status);
    }

    int remaining = number_of_workers_remaining(rank, number_of_workers_per_cluster);
    int number_of_elements_per_worker = number_of_elements / remaining;
    int modulo = number_of_elements % remaining;
    int bonus_work = (modulo > number_of_workers_per_cluster[rank]) ? 
        number_of_workers_per_cluster[rank] : modulo;

    // If the number in the array does not divide equally, then some workers
    // will receive one more number to deal with
    int elements_to_workers = number_of_elements_per_worker * number_of_workers_per_cluster[rank] +
        bonus_work;
    int elements_left = number_of_elements - elements_to_workers;

    // The last coordinator in the sequence doesn't send further the work
    if (rank > last_rank || rank == FIRST_COORDINATOR) {
        send_work_to_next_coordinator(rank, elements_left, number_of_elements, v);
    }
    
    // We send to the workers their share of work
    send_and_recv(rank, number_of_elements_per_worker, 
    number_of_workers_per_cluster[rank], topology, v, bonus_work);

    if (rank != FIRST_COORDINATOR && rank >= last_rank) {
        forward_results(rank, elements_to_workers, v, number_of_elements, last_rank);
    }
    else {
        int already_received = elements_to_workers;
        for (int i = 0; i < LAST_COORDINATOR - (last_rank - 1); i++) {
            int number;
            MPI_Recv(&number, 1, MPI_INT, LAST_COORDINATOR, 1, MPI_COMM_WORLD, &status);
            MPI_Recv((v + already_received), number, MPI_INT, LAST_COORDINATOR, 1, MPI_COMM_WORLD, &status);
            already_received += number;
        }
    }
}

int main (int argc, char *argv[])
{
    int numtasks, rank;
    int coordinator_rank = -1; // If the process is a coordinator, then this is -1
    int *topology[NUMBER_OF_COORDINATORS];
    int number_of_workers_per_cluster[NUMBER_OF_COORDINATORS];
    int number_of_elements;
    int comm_error;
    int *v;

    if (argc != 3) {
        printf("Bad execution, please try again.\n");
        exit(1);
    }

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
    MPI_Comm_rank(MPI_COMM_WORLD,&rank);

    comm_error = atoi(argv[2]);

    for (int i = 0; i < NUMBER_OF_COORDINATORS; i++){
        number_of_workers_per_cluster[i] = 0;
    }

    // We find out the topology
    if (rank_is_coordinator(rank)) {
        topology[rank] = read_workers_from_file(rank, &number_of_workers_per_cluster[rank]);

        send_hello_message(rank, topology[rank], number_of_workers_per_cluster[rank]);

        if (comm_error == 0){
            find_topology_coordinator(rank, topology, number_of_workers_per_cluster);
        }
        else if (comm_error == 1){
            find_topology_coordinator_broken_link(rank, number_of_workers_per_cluster, topology, 1);
        }
        else {
            find_topology_coordinator_broken_link(rank, number_of_workers_per_cluster, topology, 2);
        }
        send_topology_to_workers(rank, topology, number_of_workers_per_cluster);
    }
    else {
        coordinator_rank = recv_hello_message();
        recv_topology_from_coordinator(topology, number_of_workers_per_cluster, coordinator_rank);
    }

    print_topology(rank, topology, number_of_workers_per_cluster);

    if (rank == FIRST_COORDINATOR) {
        number_of_elements = atoi(argv[1]);
        v = allocate_array(number_of_elements);

        for (int i = 0; i < number_of_elements; i++) {
            v[i] = number_of_elements - i - 1;
        }
    }

    
    if (rank_is_coordinator(rank)) {
        if (comm_error != 2) {
            coordinate_work(rank, number_of_elements, number_of_workers_per_cluster, topology, v, 1);
        }
        else {
            if (rank != 1) {
                coordinate_work(rank, number_of_elements, number_of_workers_per_cluster, topology, v, 2);
            }
        }
    }
    else {
        if (comm_error != 2 || coordinator_rank != 1)
        make_work(rank, coordinator_rank);
    }

    if (rank == FIRST_COORDINATOR) {
       print_result(v, number_of_elements);
       free(v);
    }

    MPI_Finalize();
    return 0;
}