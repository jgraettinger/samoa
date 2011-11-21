
    /*!
     *
     *
    class value_zci_adapter :
        public google::protobuf::io::ZeroCopyInputStream
    {
    public:

        value_zci_adapter(hash_ring_element &);

        bool Next(void ** data, int * size);

        void BackUp(int count);

        bool Skip(int count);

        google::protobuf::int64 ByteCount() const;

    private:
        hash_ring_element & _element;
    };
    */

