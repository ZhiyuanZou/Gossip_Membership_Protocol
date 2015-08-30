/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
#include <string>
#include <iostream>
using std::string;
using std::cout;
using std::endl;
const int thresh=50;
/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */

bool check_entry(MemberListEntry& my, MemberListEntry& other){
    if(my.id==other.id&&my.port==other.port) return true;
    else return false;
}

MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
    for( int i = 0; i < 6; i++ ) {
        NULLADDR[i] = 0;
    }
    this->memberNode = member;
    this->emulNet = emul;
    this->log = log;
    this->par = params;
    this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
        return false;
    }
    else {
        return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
    Queue q;
    return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();
    
    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }
    
    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }
    
    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
    /*
     * This function is partially implemented and may require changes
     */
    int id = *(int*)(&memberNode->addr.addr);
    int port = *(short*)(&memberNode->addr.addr[4]);
    
    memberNode->bFailed = false;
    memberNode->inited = true;
    memberNode->inGroup = false;
    // node is up!
    memberNode->nnb = 0;
    memberNode->heartbeat = 0;
    memberNode->pingCounter = TFAIL;
    memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);
    
    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
    Msg_joinreq *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif
    
    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        /*
         size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
         msg = (MessageHdr *) malloc(msgsize * sizeof(char));
         
         // create JOINREQ message: format of data is {struct Address myaddr}
         msg->msgType = JOINREQ;
         memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
         memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));
         */
        size_t msgsize=sizeof(Msg_joinreq);
        msg=new Msg_joinreq();
        msg->msgType=JOINREQ;
        memcpy(&msg->addr,&memberNode->addr.addr,sizeof(memberNode->addr.addr));
        //   cout<<"test sending addr "<<memberNode->addr.getAddress()<<endl;
        msg->heartbeat=memberNode->heartbeat;
        
#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif
        
        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);
        
        free(msg);
    }
    
    return 1;
    
}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
    /*
     * Your code goes here
     */
    memberNode->bFailed = false;
    memberNode->inGroup = false;
    return 1;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
        return;
    }
    
    // Check my messages
    checkMessages();
    
    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
        return;
    }
    
    // ...then jump in and share your responsibilites!
    nodeLoopOps();
    
    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;
    
    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
        ptr = memberNode->mp1q.front().elt;
        size = memberNode->mp1q.front().size;
        memberNode->mp1q.pop();
        
        recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::joinreq___handler(void *env, Msg_joinreq *data, int size ){
    //long* l=(long*)(data+1+sizeof(Address));
    Member* mptr=(Member*) env;
    static char s[1024];
    /**** if the memberList is full, remove one of them other than yourself   ***/
    /**
     if(mptr->memberList.size()>=10) {
    	if(mptr->myPos!=mptr->memberList.begin()) mptr->memberList.erase(mptr->memberList.begin()) ;
    	else mptr->memberList.erase(mptr->myPos+1);
     }
     /****** add a new memberlist entry ******/
    MemberListEntry* mlist=new MemberListEntry();
    int id = 0;
    short port;
    memcpy(&id, &(data->addr)[0], sizeof(int));
    memcpy(&port, &(data->addr)[4], sizeof(short));
    mlist->id=id;
    mlist->port=port;
    mlist->heartbeat=data->heartbeat;
    
    /**** put the new entry's timestamp to be the heartbeat, in case you need to update it ***/
    mlist->timestamp=mptr->heartbeat;
    
    bool found=false;
    for(auto it:mptr->memberList){
        if(check_entry(it,*mlist)){
            found=true;
            if(mlist->heartbeat>it.heartbeat){
                it.heartbeat=mlist->heartbeat;
                it.timestamp=mptr->heartbeat;
            }
            break;
        }
    }
    if(!found) mptr->memberList.push_back(*mlist);
    //  (*(memberNode->myPos)).heartbeat=memberNode->heartbeat;
    /**** logadd ****/
    string str=to_string(id) + ":" + to_string(port);
    Address a(str);
    log->logNodeAdd(&(mptr->addr),&a);
    /****** send the msg_joinrep   *******/
    Msg_joinrep * msg=new Msg_joinrep();
    msg->msgType=JOINREP;
    for(auto it: mptr->memberList){
        msg->memberList.push_back(it);
    }
    size_t msgsize=sizeof(*msg);
#ifdef DEBUGLOG
    sprintf(s, "Response to join request from:");
    log->LOG(&a, s);
#endif
    
    // send JOINREQ message to introducer member
    emulNet->ENsend(&memberNode->addr, &a, (char *)msg, msgsize);
    
    //    sprintf(s, "joinreq_handler for address:");
    //    log->LOG(&(((Member*)env)->addr), s);
    //	  cout<<"heartbeat is "<<data->heartbeat<<endl;
    return true;
}

bool MP1Node::joinrep___handler(void *env, Msg_joinrep *data, int size){
    Member* mptr=(Member*) env;
    //need to put myself in the membership list, use myPos to update it before sending the membership list to others
    int current_time=mptr->heartbeat;
    for(auto it:data->memberList){
        bool found=false;
        auto myentry=mptr->memberList.begin();
        for(auto myit=mptr->memberList.begin();myit!=mptr->memberList.end();++myit){
            if(check_entry(*myit,it)){
                found=true;
                myentry=myit;
            }
        }
        if(!found){
            it.timestamp=mptr->heartbeat;
            mptr->memberList.push_back(it);
            int id = it.id;
            short port=it.port;
            string str=to_string(id) + ":" + to_string(port);
            Address a(str);
            log->logNodeAdd(&(mptr->addr),&a);
    				}
        else{
            if((*myentry).heartbeat<it.heartbeat){
                //             cout<<it.heartbeat<<endl;
                //                if(it.heartbeat>400)
                //                    cout<<""<<endl;
                myentry->heartbeat=it.heartbeat;
                myentry->timestamp=mptr->heartbeat;
            }
        }
    }
    
    
    //mptr->memberList=data->memberList;
    mptr->nnb=mptr->memberList.size()-1;
    mptr->inGroup=true;
    //    mptr->myPos=mptr->memberList.begin();
    return true;
}



bool MP1Node::recvCallBack(void *env, char *data, int size ) {
    /*
     * Your code goes here
     */
    switch(((Msg_joinreq*)data)->msgType){
        case JOINREQ:{
            return joinreq___handler(env,(Msg_joinreq*)data,size);
        }
        case JOINREP:{
            auto check1=(Msg_joinrep*) data;
            return joinrep___handler(env,(Msg_joinrep*)data,size);
        }
        default:{return true;}
    }
    
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    
    /*
     * Your code goes here
     */
    /** update my heartbeat **/
    ++memberNode->heartbeat;
    int current_time=memberNode->heartbeat;
    for(auto it=memberNode->memberList.begin(); it!=memberNode->memberList.end(); ++it){
        int id = 0;
        short port;
        memcpy(&id, &memberNode->addr.addr[0], sizeof(int));
        memcpy(&port, &memberNode->addr.addr[4], sizeof(short));
        if(it->getid()==id&&it->getport()==port){
            memberNode->myPos=it;
        }
    }
    /**** update the heartbeat of myPos  ***/
    (*(memberNode->myPos)).heartbeat=memberNode->heartbeat;
    (*(memberNode->myPos)).timestamp=memberNode->heartbeat;
    /**** if the node has no neighbour ****/
    if(memberNode->memberList.size()==1) {}
    else{
        /**** randomly pick a neighbour  ****/
        //        auto sz=memberNode->memberList.size()-1;
        //        auto pos=rand()%sz+1;
        //        MemberListEntry& mentry=*(memberNode->memberList.begin()+pos);
        //        string str= to_string(mentry.id) + ":" + to_string(mentry.port);
        //        Address a(str);
        
        /*** build the msg ****/
        Msg_joinrep * msg=new Msg_joinrep();
        msg->msgType=JOINREP;
        for(auto it: memberNode->memberList){
            if(current_time-it.timestamp<thresh)
                msg->memberList.push_back(it);
        }
        //      size_t msgsize=sizeof(*msg)+msg->memberList.size()*sizeof(MemberListEntry);
        size_t msgsize=sizeof(*msg);
        
        for(auto it:memberNode->memberList){
            string str= to_string(it.id) + ":" + to_string(it.port);
            Address a(str);
            // send JOINREQ message to introducer member
            if(rand()%2==1){
                emulNet->ENsend(&memberNode->addr, &a, (char *)msg, msgsize);
            }
        }
    }
    /** problem **/
    for(auto bit=memberNode->memberList.begin(); bit!=memberNode->memberList.end();++bit){
        if(memberNode->memberList.size()==1) {
            cout<<"bug"<<endl;
            // break;
        }
        if(current_time-(*bit).timestamp>2*thresh){
            // memberNode->memberList.erase(bit);
            string str= to_string((*bit).id) + ":" + to_string((*bit).port);
            cout<<"remove "<<str<<endl;
            Address a(str);
            if(bit==memberNode->myPos)
                cout<<""<<endl;
            log->logNodeRemove(&(memberNode->addr),&a);
            *bit=*(memberNode->memberList.end()-1);
            memberNode->memberList.pop_back();
            break;
        }
    }
    
    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
    return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;
    
    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;
    
    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
    memberNode->memberList.clear();
    
    /** put self into the memberlist ***/
    int id = 0;
    short port;
    memcpy(&id, &(memberNode->addr.addr)[0], sizeof(int));
    memcpy(&port, &(memberNode->addr.addr)[4], sizeof(short));
    MemberListEntry mlist;
    mlist.id=id;
    mlist.port=port;
    mlist.heartbeat=0;
    mlist.timestamp=0;
    memberNode->memberList.push_back(mlist);
    memberNode->myPos=memberNode->memberList.begin();
    
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
           addr->addr[3], *(short*)&addr->addr[4]) ;
}
