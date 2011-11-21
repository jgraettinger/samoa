
    /*!
     *
     *
    class value_zco_adapter :
        public google::protobuf::io::ZeroCopyOutputStream
    {
    public:

        value_zco_adapter(hash_ring_element &);
        ~value_zco_adapter();

        bool Next(void ** data, int * size);

        void BackUp(int count);

        google::protobuf::int64 ByteCount() const;

    private:
        hash_ring_element & _element;
    };
    /

